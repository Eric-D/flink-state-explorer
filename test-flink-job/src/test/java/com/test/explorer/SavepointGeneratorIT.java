package com.test.explorer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end savepoint generator backed by Testcontainers.
 *
 * <p>Spins up a real Flink 1.20 cluster (JobManager + TaskManager) in Docker, submits
 * {@link ClusterJob} (which exercises every state type the explorer understands), triggers a
 * CANONICAL savepoint, and copies it onto the host so the Rust tests can read it directly.
 *
 * <p>Run it standalone with:
 * <pre>
 *   mvn -q verify                       # build the shaded jar, then generate the savepoint
 * </pre>
 * The savepoint lands in {@code ../tests/fixtures/savepoint-gitlab} by default (override with
 * {@code -Dsavepoint.output.dir=...}), which is exactly what {@code tests/gitlab_savepoint_test.rs}
 * reads. Requires a working Docker daemon.
 *
 * <p>Notes:
 * <ul>
 *   <li>The cluster uses JobManager checkpoint storage ({@code execution.checkpointing.storage:
 *       jobmanager}, Flink's default without {@code execution.checkpointing.dir}): its
 *       {@code MemCheckpointStreamFactory} returns every handle inline, whatever the inline
 *       threshold says, so the savepoint is a single self-contained {@code _metadata} produced by
 *       the JobManager — no shared volume between JM and TM is needed. Each handle is capped at
 *       5 MiB ("Size of the state is larger than the maximum permitted memory-backed state").
 *       {@code BigSavepointGeneratorIT} uses filesystem storage instead, to get state files.</li>
 *   <li>The savepoint is copied out of the container with the Docker API, so the host-side files
 *       are owned by the current user (a bind mount would leave them owned by the in-container
 *       {@code flink} user, which breaks {@code mvn clean}).</li>
 *   <li>{@code -Dapi.version} is pinned in the failsafe config because docker-java otherwise
 *       negotiates Docker API v1.32, which daemons >= 25 reject.</li>
 * </ul>
 */
@Testcontainers
class SavepointGeneratorIT {

    private static final DockerImageName FLINK_IMAGE = DockerImageName.parse("flink:1.20-java11");
    private static final String ENTRY_CLASS = "com.test.explorer.ClusterJob";
    // A path the in-container `flink` user (uid 9999) can create — `/` is root-owned, so a
    // top-level dir like /savepoints can't be made without a bind mount/volume providing it.
    private static final String SAVEPOINTS_DIR = "/tmp/flink-savepoints";
    private static final int PARALLELISM = 2;

    /**
     * Mirrors test-flink-job/docker-compose.yml. JobManager checkpoint storage (Flink's default when
     * no {@code execution.checkpointing.dir} is set, spelled out here) keeps every state handle inline,
     * so the savepoint is one self-contained {@code _metadata}; it caps each handle at 5 MiB.
     */
    private static final String FLINK_PROPERTIES = String.join("\n",
            "jobmanager.rpc.address: jobmanager",
            "state.savepoints.dir: file://" + SAVEPOINTS_DIR,
            "state.backend: hashmap",
            "execution.checkpointing.storage: jobmanager",
            "taskmanager.numberOfTaskSlots: 4");

    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final GenericContainer<?> JOBMANAGER = new GenericContainer<>(FLINK_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("jobmanager")
            .withCommand("jobmanager")
            .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
            .withExposedPorts(8081)
            .waitingFor(Wait.forHttp("/overview").forPort(8081).withStartupTimeout(Duration.ofMinutes(3)));

    @Container
    static final GenericContainer<?> TASKMANAGER = new GenericContainer<>(FLINK_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("taskmanager")
            .withCommand("taskmanager")
            .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
            .dependsOn(JOBMANAGER);

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void generatesCanonicalSavepointWithAllStateTypes() throws Exception {
        String rest = "http://" + JOBMANAGER.getHost() + ":" + JOBMANAGER.getMappedPort(8081);
        try {
            awaitTaskManagerRegistered(rest);

            Path jar = findJobJar();
            System.out.println("Using job jar: " + jar);

            String jarId = uploadJar(rest, jar);
            String jobId = runJob(rest, jarId);
            System.out.println("Submitted job " + jobId);

            awaitJobRunning(rest, jobId);
            System.out.println("Job RUNNING; waiting for state population...");
            Thread.sleep(6_000);

            String location = triggerCanonicalSavepoint(rest, jobId);
            System.out.println("Savepoint completed at (in-container): " + location);

            Path out = resolveOutputDir();
            copySavepointFromContainer(new URI(location).getPath(), out);

            Path metadata = out.resolve("_metadata");
            assertTrue(Files.exists(metadata), "_metadata must exist in " + out);
            long size = Files.size(metadata);
            assertTrue(size > 100, "_metadata should be non-trivial, was " + size + " bytes");
            long fileCount;
            try (Stream<Path> s = Files.list(out)) {
                fileCount = s.count();
            }
            System.out.println("==> Savepoint written to " + out.toAbsolutePath()
                    + " (_metadata " + size + " bytes, " + fileCount + " files)");
            cancelJob(rest, jobId);
        } catch (Exception | AssertionError e) {
            dumpLogs();
            throw e;
        }
    }

    /**
     * Same job, but with snapshot compression on, into {@code tests/fixtures/savepoint-compressed}.
     *
     * <p>Without this fixture the compressed read path is only covered by a Snappy frame built in
     * a Rust unit test — never by bytes Flink actually wrote. Each key-group becomes its own
     * Snappy-framed stream and the keyed header's {@code usingKeyGroupCompression} flips to true;
     * {@code tests/compressed_savepoint_test.rs} asserts both.
     */
    @Test
    void generatesCompressedSavepoint() throws Exception {
        String rest = "http://" + JOBMANAGER.getHost() + ":" + JOBMANAGER.getMappedPort(8081);
        try {
            awaitTaskManagerRegistered(rest);

            Path jar = findJobJar();
            String jarId = uploadJar(rest, jar);
            String jobId = runJob(rest, jarId, "compress");
            System.out.println("Submitted compressed job " + jobId);

            awaitJobRunning(rest, jobId);
            Thread.sleep(6_000);

            String location = triggerCanonicalSavepoint(rest, jobId);
            Path out = resolveOutputDir("savepoint.compressed.output.dir",
                    "../tests/fixtures/savepoint-compressed");
            copySavepointFromContainer(new URI(location).getPath(), out);

            Path metadata = out.resolve("_metadata");
            assertTrue(Files.exists(metadata), "_metadata must exist in " + out);
            long size = Files.size(metadata);
            assertTrue(size > 100, "_metadata should be non-trivial, was " + size + " bytes");
            System.out.println("==> Compressed savepoint written to " + out.toAbsolutePath()
                    + " (_metadata " + size + " bytes)");
            cancelJob(rest, jobId);
        } catch (Exception | AssertionError e) {
            dumpLogs();
            throw e;
        }
    }

    // --- Flink REST helpers (formats mirror test-flink-job/run-on-cluster.sh) -----------------

    private void awaitTaskManagerRegistered(String rest) throws Exception {
        for (int i = 0; i < 60; i++) {
            JsonNode overview = mapper.readTree(get(rest + "/overview"));
            if (overview.path("taskmanagers").asInt() >= 1
                    && overview.path("slots-total").asInt() >= PARALLELISM) {
                return;
            }
            Thread.sleep(1_000);
        }
        throw new IllegalStateException("No TaskManager registered with the JobManager in time");
    }

    private String uploadJar(String rest, Path jar) throws Exception {
        String boundary = "----flinkBoundary" + System.nanoTime();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        String head = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + jar.getFileName() + "\"\r\n"
                + "Content-Type: application/java-archive\r\n\r\n";
        body.write(head.getBytes(StandardCharsets.UTF_8));
        body.write(Files.readAllBytes(jar));
        body.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));

        HttpRequest req = HttpRequest.newBuilder(URI.create(rest + "/jars/upload"))
                .timeout(Duration.ofMinutes(2))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "Jar upload failed: " + resp.body());
        // "filename" is the absolute path on the JM; the jar id is its basename.
        String filename = mapper.readTree(resp.body()).get("filename").asText();
        return filename.substring(filename.lastIndexOf('/') + 1);
    }

    private String runJob(String rest, String jarId) throws Exception {
        return runJob(rest, jarId, null);
    }

    /** `programArg` is passed to {@link ClusterJob#main} — "compress" enables snapshot compression. */
    private String runJob(String rest, String jarId, String programArg) throws Exception {
        String args = programArg == null ? "" : ",\"programArgsList\":[\"" + programArg + "\"]";
        String payload = "{\"entryClass\":\"" + ENTRY_CLASS + "\",\"parallelism\":" + PARALLELISM
                + args + "}";
        JsonNode resp = mapper.readTree(post(rest + "/jars/" + jarId + "/run", payload));
        return resp.get("jobid").asText();
    }

    /**
     * Cancels the job so its slots are free for the next test's job: both tests share one
     * cluster, and a savepoint does not stop the job it was taken from.
     */
    private void cancelJob(String rest, String jobId) {
        try {
            post(rest + "/jobs/" + jobId + "/stop", "{\"drain\":false}");
        } catch (Exception e) {
            System.out.println("Could not stop job " + jobId + ": " + e.getMessage());
        }
    }

    private void awaitJobRunning(String rest, String jobId) throws Exception {
        for (int i = 0; i < 60; i++) {
            String state = mapper.readTree(get(rest + "/jobs/" + jobId)).path("state").asText();
            if ("RUNNING".equals(state)) {
                return;
            }
            if ("FAILED".equals(state) || "CANCELED".equals(state) || "FINISHED".equals(state)) {
                throw new IllegalStateException("Job reached terminal state before savepoint: " + state
                        + "\n" + safeGet(rest + "/jobs/" + jobId + "/exceptions"));
            }
            Thread.sleep(1_000);
        }
        throw new IllegalStateException("Job did not reach RUNNING in time");
    }

    private String triggerCanonicalSavepoint(String rest, String jobId) throws Exception {
        String payload = "{\"cancel-job\":true,\"formatType\":\"CANONICAL\","
                + "\"target-directory\":\"file://" + SAVEPOINTS_DIR + "\"}";
        String triggerId = mapper.readTree(post(rest + "/jobs/" + jobId + "/savepoints", payload))
                .get("request-id").asText();

        for (int i = 0; i < 90; i++) {
            JsonNode status = mapper.readTree(get(rest + "/jobs/" + jobId + "/savepoints/" + triggerId));
            String state = status.path("status").path("id").asText();
            if ("COMPLETED".equals(state)) {
                JsonNode op = status.path("operation");
                if (op.has("failure-cause")) {
                    throw new IllegalStateException("Savepoint failed: " + op.get("failure-cause"));
                }
                return op.path("location").asText();
            }
            Thread.sleep(1_000);
        }
        throw new IllegalStateException("Savepoint did not complete in time");
    }

    // --- HTTP plumbing ------------------------------------------------------------------------

    private String get(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(30)).GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("GET " + url + " -> " + resp.statusCode() + ": " + resp.body());
        }
        return resp.body();
    }

    private String safeGet(String url) {
        try {
            return get(url);
        } catch (Exception e) {
            return "(could not fetch " + url + ": " + e.getMessage() + ")";
        }
    }

    private String post(String url, String json) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200 && resp.statusCode() != 202) {
            throw new IOException("POST " + url + " -> " + resp.statusCode() + ": " + resp.body());
        }
        return resp.body();
    }

    // --- Extraction / filesystem helpers ------------------------------------------------------

    private static Path findJobJar() throws IOException {
        Path target = Paths.get("target");
        try (Stream<Path> s = Files.list(target)) {
            return s.filter(p -> {
                        String n = p.getFileName().toString();
                        return n.startsWith("flink-savepoint-generator") && n.endsWith(".jar")
                                && !n.contains("original");
                    })
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Shaded job jar not found in target/ — run `mvn package` first"));
        }
    }

    private static Path resolveOutputDir() throws IOException {
        return resolveOutputDir("savepoint.output.dir", "../tests/fixtures/savepoint-gitlab");
    }

    private static Path resolveOutputDir(String property, String defaultDir) throws IOException {
        Path out = Paths.get(System.getProperty(property, defaultDir)).toAbsolutePath().normalize();
        if (Files.exists(out)) {
            deleteRecursively(out);
        }
        Files.createDirectories(out);
        return out;
    }

    /**
     * Copies every file in the savepoint directory out of the JobManager container via the Docker
     * API (host-side files are owned by the current user). Canonical/hashmap savepoint directories
     * are flat, so a single-level listing is sufficient.
     */
    private void copySavepointFromContainer(String savepointDirInContainer, Path dest) throws Exception {
        ExecResult ls = JOBMANAGER.execInContainer("sh", "-c", "ls -1 " + savepointDirInContainer);
        if (ls.getExitCode() != 0) {
            throw new IllegalStateException("Could not list " + savepointDirInContainer
                    + ": " + ls.getStderr());
        }
        int copied = 0;
        for (String name : ls.getStdout().split("\\R")) {
            if (name.isBlank()) {
                continue;
            }
            JOBMANAGER.copyFileFromContainer(
                    savepointDirInContainer + "/" + name,
                    dest.resolve(name).toString());
            copied++;
        }
        if (copied == 0) {
            throw new IllegalStateException("Savepoint directory was empty: " + savepointDirInContainer);
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void dumpLogs() {
        try {
            System.err.println("=== JobManager logs ===\n" + JOBMANAGER.getLogs());
            System.err.println("=== TaskManager logs ===\n" + TASKMANAGER.getLogs());
        } catch (Exception ignored) {
            // best effort
        }
    }
}
