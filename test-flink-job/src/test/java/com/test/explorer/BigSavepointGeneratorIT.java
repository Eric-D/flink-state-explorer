package com.test.explorer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OPT-IN generator for a LARGE savepoint ({@code ../tests/fixtures/savepoint-big}, gitignored),
 * so the explorer's read performance can be measured at scale and so keyed state that spills to
 * separate files ({@code RelativeFileStateHandle}, i.e. {@code StreamStateHandle::Relative} on the
 * Rust side) is exercised for the first time — the other fixtures inline everything into
 * {@code _metadata}.
 *
 * <p>It is skipped unless {@code -Dbig.savepoint=true}, and starts its containers itself (no
 * {@code @Testcontainers}) so a plain {@code mvn verify} does not even pull up a cluster for it:
 * <pre>
 *   cd test-flink-job
 *   mvn -B -Dbig.savepoint=true -Dit.test=BigSavepointGeneratorIT verify
 *   # optional: -Dbig.savepoint.keys=2000000 -Dbig.savepoint.tm.memory=6g
 *   #           -Dbig.savepoint.output.dir=/abs/path
 * </pre>
 *
 * <p>How it differs from {@link SavepointGeneratorIT}:
 * <ul>
 *   <li>runs {@link BigStateJob} (N String keys × {@code ValueState<Long>} +
 *       {@code ValueState<String>} + small {@code MapState<String,Long>}), parallelism 2, default
 *       max parallelism 128;</li>
 *   <li>the cluster uses {@code FileSystemCheckpointStorage} (the other ITs sit on the default
 *       {@code JobManagerCheckpointStorage}, which caps any handle at 5 MB whatever the memory
 *       threshold says) and {@code state.storage.fs.memory-threshold} is left at its DEFAULT
 *       (20 kb), so each subtask's keyed state is written as its own file next to
 *       {@code _metadata};</li>
 *   <li>the TaskManager gets a bigger heap ({@code taskmanager.memory.process.size}, default 4g,
 *       with managed memory shrunk since the hashmap backend keeps state on the heap) so a few
 *       million heap-backend keys fit;</li>
 *   <li>JM and TM share no volume, so the JM writes {@code _metadata} and the TM writes the state
 *       files to the SAME {@code savepoint-<id>} path on their own filesystems; both are copied
 *       into one flat host directory, which is the layout a relative handle resolves against;</li>
 *   <li>the savepoint is triggered only once the process vertex's {@code read-records} REST metric
 *       reaches N, instead of after a fixed sleep.</li>
 * </ul>
 */
class BigSavepointGeneratorIT {

    private static final DockerImageName FLINK_IMAGE = DockerImageName.parse("flink:1.20-java11");
    private static final String ENTRY_CLASS = "com.test.explorer.BigStateJob";
    private static final String SAVEPOINTS_DIR = "/tmp/flink-savepoints";
    private static final String CHECKPOINTS_DIR = "/tmp/flink-checkpoints";
    private static final int PARALLELISM = 2;
    private static final Duration FILL_TIMEOUT = Duration.ofMinutes(10);
    private static final Duration SAVEPOINT_TIMEOUT = Duration.ofMinutes(10);
    /** A deliberately loose lower bound (~half the measured ~210 B/key) to catch a silently empty run. */
    private static final long MIN_BYTES_PER_KEY = 100;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    private static String flinkProperties(String tmProcessMemory) {
        return String.join("\n",
                "jobmanager.rpc.address: jobmanager",
                "state.savepoints.dir: file://" + SAVEPOINTS_DIR,
                "state.backend: hashmap",
                // The other ITs run on the DEFAULT checkpoint storage (JobManagerCheckpointStorage),
                // which keeps every handle in memory and caps it at 5 MB ("Size of the state is
                // larger than the maximum permitted memory-backed state") — the fs memory threshold
                // does not even apply to it. A ~100 MB-per-subtask state needs FileSystemCheckpointStorage.
                "execution.checkpointing.storage: filesystem",
                "execution.checkpointing.dir: file://" + CHECKPOINTS_DIR,
                // NO state.storage.fs.memory-threshold override: the default (20 kb) is the point.
                "jobmanager.memory.process.size: 2g",
                "taskmanager.memory.process.size: " + tmProcessMemory,
                // hashmap backend = state on the JVM heap; managed memory would be dead weight.
                "taskmanager.memory.managed.fraction: 0.1",
                "taskmanager.numberOfTaskSlots: 4");
    }

    @Test
    void generatesBigSavepointWithSpilledKeyedState() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("big.savepoint"),
                "opt-in fixture: run with -Dbig.savepoint=true");

        int keys = Integer.getInteger("big.savepoint.keys", BigStateJob.DEFAULT_KEYS);
        String tmMemory = System.getProperty("big.savepoint.tm.memory", "4g");
        String properties = flinkProperties(tmMemory);
        System.out.println("Big savepoint: " + keys + " keys, TM process memory " + tmMemory);

        try (Network network = Network.newNetwork();
             GenericContainer<?> jobmanager = new GenericContainer<>(FLINK_IMAGE)
                     .withNetwork(network)
                     .withNetworkAliases("jobmanager")
                     .withCommand("jobmanager")
                     .withEnv("FLINK_PROPERTIES", properties)
                     .withExposedPorts(8081)
                     .waitingFor(Wait.forHttp("/overview").forPort(8081)
                             .withStartupTimeout(Duration.ofMinutes(3)));
             GenericContainer<?> taskmanager = new GenericContainer<>(FLINK_IMAGE)
                     .withNetwork(network)
                     .withNetworkAliases("taskmanager")
                     .withCommand("taskmanager")
                     .withEnv("FLINK_PROPERTIES", properties)) {

            long t0 = System.nanoTime();
            jobmanager.start();
            taskmanager.start();
            String rest = "http://" + jobmanager.getHost() + ":" + jobmanager.getMappedPort(8081);
            try {
                awaitTaskManagerRegistered(rest);
                System.out.println("Cluster up in " + seconds(t0) + "s");

                Path jar = findJobJar();
                String jarId = uploadJar(rest, jar);
                String jobId = runJob(rest, jarId, String.valueOf(keys));
                System.out.println("Submitted big job " + jobId);
                awaitJobRunning(rest, jobId);

                long t1 = System.nanoTime();
                awaitRecordsConsumed(rest, jobId, keys);
                System.out.println("State populated (" + keys + " keys) in " + seconds(t1) + "s");

                long t2 = System.nanoTime();
                String location = triggerCanonicalSavepoint(rest, jobId);
                System.out.println("Savepoint completed in " + seconds(t2) + "s at (in-container): "
                        + location);

                long t3 = System.nanoTime();
                String dir = new URI(location).getPath();
                Path out = resolveOutputDir();
                int fromJm = copySavepointFromContainer(jobmanager, dir, out);
                int fromTm = copySavepointFromContainer(taskmanager, dir, out);
                System.out.println("Copied " + fromJm + " file(s) from the JobManager and " + fromTm
                        + " from the TaskManager in " + seconds(t3) + "s");

                // --- Checks ---------------------------------------------------------------
                Path metadata = out.resolve("_metadata");
                assertTrue(Files.exists(metadata), "_metadata must exist in " + out);
                List<Path> files;
                try (Stream<Path> s = Files.list(out)) {
                    files = s.sorted().collect(java.util.stream.Collectors.toList());
                }
                long total = 0;
                StringBuilder listing = new StringBuilder();
                for (Path f : files) {
                    long size = Files.size(f);
                    total += size;
                    listing.append(String.format("    %12d  %s%n", size, f.getFileName()));
                }
                System.out.print(listing);
                assertTrue(fromTm >= 1,
                        "expected at least one keyed-state file written by the TaskManager (state "
                                + "above state.storage.fs.memory-threshold); got only " + listing);
                assertTrue(total >= keys * MIN_BYTES_PER_KEY,
                        "savepoint is suspiciously small for " + keys + " keys: " + total + " bytes");
                System.out.println("==> Big savepoint written to " + out.toAbsolutePath()
                        + " (" + files.size() + " files, " + total + " bytes, "
                        + (total / keys) + " B/key, " + seconds(t0) + "s total)");
            } catch (Exception | AssertionError e) {
                dumpLogs(jobmanager, taskmanager);
                throw e;
            }
        }
    }

    // --- Flink REST helpers (same shapes as SavepointGeneratorIT) -----------------------------

    private void awaitTaskManagerRegistered(String rest) throws Exception {
        for (int i = 0; i < 120; i++) {
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
        String filename = mapper.readTree(resp.body()).get("filename").asText();
        return filename.substring(filename.lastIndexOf('/') + 1);
    }

    /** {@code programArg} is {@link BigStateJob#main}'s first argument: the key count. */
    private String runJob(String rest, String jarId, String programArg) throws Exception {
        String payload = "{\"entryClass\":\"" + ENTRY_CLASS + "\",\"parallelism\":" + PARALLELISM
                + ",\"programArgsList\":[\"" + programArg + "\"]}";
        JsonNode resp = mapper.readTree(post(rest + "/jars/" + jarId + "/run", payload));
        return resp.get("jobid").asText();
    }

    private void awaitJobRunning(String rest, String jobId) throws Exception {
        for (int i = 0; i < 120; i++) {
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

    /**
     * Waits until the keyed process vertex has read {@code expected} records: processElement runs
     * synchronously on receive, so at that point every key's state is written. The JM refreshes
     * these metrics every ~10 s ({@code metrics.fetcher.update-interval}), hence the lag.
     */
    private void awaitRecordsConsumed(String rest, String jobId, long expected) throws Exception {
        long deadline = System.nanoTime() + FILL_TIMEOUT.toNanos();
        long lastReported = -1;
        while (System.nanoTime() < deadline) {
            JsonNode job = mapper.readTree(get(rest + "/jobs/" + jobId));
            String state = job.path("state").asText();
            if (!"RUNNING".equals(state)) {
                throw new IllegalStateException("Job left RUNNING while filling state: " + state
                        + "\n" + safeGet(rest + "/jobs/" + jobId + "/exceptions"));
            }
            long read = -1;
            for (JsonNode vertex : job.path("vertices")) {
                if (vertex.path("name").asText().startsWith(BigStateJob.PROCESS_NAME)) {
                    read = vertex.path("metrics").path("read-records").asLong(-1);
                }
            }
            if (read != lastReported) {
                System.out.println("  " + BigStateJob.PROCESS_NAME + " read-records: " + read
                        + " / " + expected);
                lastReported = read;
            }
            if (read >= expected) {
                return;
            }
            Thread.sleep(2_000);
        }
        throw new IllegalStateException("Process vertex did not consume " + expected
                + " records within " + FILL_TIMEOUT + " (last seen " + lastReported + ")");
    }

    private String triggerCanonicalSavepoint(String rest, String jobId) throws Exception {
        String payload = "{\"cancel-job\":true,\"formatType\":\"CANONICAL\","
                + "\"target-directory\":\"file://" + SAVEPOINTS_DIR + "\"}";
        String triggerId = mapper.readTree(post(rest + "/jobs/" + jobId + "/savepoints", payload))
                .get("request-id").asText();

        long deadline = System.nanoTime() + SAVEPOINT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
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
        throw new IllegalStateException("Savepoint did not complete within " + SAVEPOINT_TIMEOUT);
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
        Path out = Paths.get(System.getProperty("big.savepoint.output.dir",
                "../tests/fixtures/savepoint-big")).toAbsolutePath().normalize();
        if (Files.exists(out)) {
            deleteRecursively(out);
        }
        Files.createDirectories(out);
        return out;
    }

    /**
     * Copies every file of {@code savepointDirInContainer} out of {@code container} into
     * {@code dest} via the Docker API (streamed, host files owned by the current user) and returns
     * how many were copied — 0 when the directory does not exist on that container (the JM only
     * ever holds {@code _metadata}, the TM only the spilled state files).
     */
    private static int copySavepointFromContainer(GenericContainer<?> container,
                                                  String savepointDirInContainer, Path dest)
            throws Exception {
        ExecResult ls = container.execInContainer("sh", "-c", "ls -1 " + savepointDirInContainer);
        if (ls.getExitCode() != 0) {
            System.out.println("  (no " + savepointDirInContainer + " on this container: "
                    + ls.getStderr().trim() + ")");
            return 0;
        }
        List<String> names = new ArrayList<>();
        for (String name : ls.getStdout().split("\\R")) {
            if (!name.isBlank()) {
                names.add(name);
            }
        }
        for (String name : names) {
            container.copyFileFromContainer(
                    savepointDirInContainer + "/" + name,
                    dest.resolve(name).toString());
        }
        return names.size();
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

    private static long seconds(long sinceNanos) {
        return (System.nanoTime() - sinceNanos) / 1_000_000_000L;
    }

    private static void dumpLogs(GenericContainer<?> jobmanager, GenericContainer<?> taskmanager) {
        try {
            System.err.println("=== JobManager logs ===\n" + jobmanager.getLogs());
            System.err.println("=== TaskManager logs ===\n" + taskmanager.getLogs());
        } catch (Exception ignored) {
            // best effort
        }
    }
}
