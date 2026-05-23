package com.test.explorer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
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
 * Generates a savepoint containing real Kafka connector state by running {@link KafkaJob}
 * (KafkaSource → KafkaSink, exactly-once) against a Testcontainers Kafka broker. The savepoint is
 * written to {@code ../tests/fixtures/savepoint-kafka} (override with {@code -Dkafka.savepoint.dir}).
 *
 * <p>Kept separate from {@link SavepointGeneratorIT} so a Kafka/broker hiccup can't break the main
 * fixture. Records are produced from inside the broker container, so only the Flink containers need
 * to reach Kafka — via the in-network listener {@code kafka:19092}.
 */
@Testcontainers
class KafkaSavepointGeneratorIT {

    private static final DockerImageName FLINK_IMAGE = DockerImageName.parse("flink:1.20-java11");
    private static final String SAVEPOINTS_DIR = "/tmp/flink-savepoints";
    private static final String KAFKA_BOOTSTRAP = "kafka:19092";
    private static final String INPUT_TOPIC = "explorer-input";
    private static final String OUTPUT_TOPIC = "explorer-output";
    private static final int PARALLELISM = 2;

    private static final String FLINK_PROPERTIES = String.join("\n",
            "jobmanager.rpc.address: jobmanager",
            "state.savepoints.dir: file://" + SAVEPOINTS_DIR,
            "state.backend: hashmap",
            "state.storage.fs.memory-threshold: 20 mb",
            "taskmanager.numberOfTaskSlots: 4");

    private static final Network NETWORK = Network.newNetwork();

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"))
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092");

    @Container
    static final GenericContainer<?> JOBMANAGER = new GenericContainer<>(FLINK_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("jobmanager")
            .withCommand("jobmanager")
            .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
            .withExposedPorts(8081)
            .dependsOn(KAFKA)
            .waitingFor(Wait.forHttp("/overview").forPort(8081).withStartupTimeout(Duration.ofMinutes(3)));

    @Container
    static final GenericContainer<?> TASKMANAGER = new GenericContainer<>(FLINK_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("taskmanager")
            .withCommand("taskmanager")
            .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
            .dependsOn(JOBMANAGER);

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void generatesKafkaSavepoint() throws Exception {
        String rest = "http://" + JOBMANAGER.getHost() + ":" + JOBMANAGER.getMappedPort(8081);
        try {
            awaitTaskManagerRegistered(rest);
            createTopics();
            startContinuousProducer(); // keep data flowing so the savepoint captures a pending txn

            Path jar = findJobJar();
            String jarId = uploadJar(rest, jar);
            String jobId = runKafkaJob(rest, jarId);
            System.out.println("Submitted Kafka job " + jobId);

            awaitJobRunning(rest, jobId);
            System.out.println("Kafka job RUNNING; letting it consume + checkpoint while producing...");
            Thread.sleep(10_000); // a few checkpoints with in-flight data → committer committables

            String location = triggerCanonicalSavepoint(rest, jobId);
            System.out.println("Kafka savepoint at (in-container): " + location);

            Path out = resolveOutputDir();
            copySavepointFromContainer(new URI(location).getPath(), out);
            long size = Files.size(out.resolve("_metadata"));
            assertTrue(size > 100, "_metadata should be non-trivial, was " + size);
            System.out.println("==> Kafka savepoint written to " + out.toAbsolutePath() + " (_metadata " + size + " bytes)");
        } catch (Exception | AssertionError e) {
            System.err.println("=== JM logs ===\n" + JOBMANAGER.getLogs());
            System.err.println("=== TM logs ===\n" + TASKMANAGER.getLogs());
            throw e;
        }
    }

    private void createTopics() throws Exception {
        exec(KAFKA, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:19092",
                "--create", "--if-not-exists", "--topic", INPUT_TOPIC, "--partitions", "3");
        exec(KAFKA, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:19092",
                "--create", "--if-not-exists", "--topic", OUTPUT_TOPIC, "--partitions", "1");
    }

    /** Produce a steady stream into the input topic (daemon thread; killed when the container stops),
     * so that at savepoint time the exactly-once sink has an open transaction → a committer committable. */
    private void startContinuousProducer() {
        Thread t = new Thread(() -> {
            try {
                KAFKA.execInContainer("/opt/kafka/bin/kafka-producer-perf-test.sh",
                        "--topic", INPUT_TOPIC, "--num-records", "1000000",
                        "--record-size", "16", "--throughput", "200",
                        "--producer-props", "bootstrap.servers=kafka:19092");
            } catch (Exception ignored) {
                // best effort — interrupted/killed when the broker container stops
            }
        }, "kafka-producer");
        t.setDaemon(true);
        t.start();
    }

    private static void exec(GenericContainer<?> c, String... cmd) throws Exception {
        var res = c.execInContainer(cmd);
        if (res.getExitCode() != 0) {
            throw new IllegalStateException("cmd failed (" + res.getExitCode() + "): " + String.join(" ", cmd)
                    + "\nstdout: " + res.getStdout() + "\nstderr: " + res.getStderr());
        }
    }

    // --- Flink REST ---------------------------------------------------------------------------

    private void awaitTaskManagerRegistered(String rest) throws Exception {
        for (int i = 0; i < 60; i++) {
            JsonNode o = mapper.readTree(get(rest + "/overview"));
            if (o.path("taskmanagers").asInt() >= 1 && o.path("slots-total").asInt() >= PARALLELISM) {
                return;
            }
            Thread.sleep(1_000);
        }
        throw new IllegalStateException("No TaskManager registered in time");
    }

    private String uploadJar(String rest, Path jar) throws Exception {
        String boundary = "----flinkBoundary" + System.nanoTime();
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(("--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + jar.getFileName() + "\"\r\n"
                + "Content-Type: application/java-archive\r\n\r\n").getBytes(StandardCharsets.UTF_8));
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

    private String runKafkaJob(String rest, String jarId) throws Exception {
        // Build the payload by hand (avoid Jackson's write path: the Kafka connector pulls an older
        // jackson-core onto the test classpath that lacks BufferRecycler.releaseToPool).
        String payload = "{\"entryClass\":\"com.test.explorer.KafkaJob\",\"parallelism\":" + PARALLELISM
                + ",\"programArgsList\":[\"" + KAFKA_BOOTSTRAP + "\",\"" + INPUT_TOPIC + "\",\"" + OUTPUT_TOPIC + "\"]}";
        JsonNode resp = mapper.readTree(post(rest + "/jars/" + jarId + "/run", payload));
        return resp.get("jobid").asText();
    }

    private void awaitJobRunning(String rest, String jobId) throws Exception {
        for (int i = 0; i < 60; i++) {
            String state = mapper.readTree(get(rest + "/jobs/" + jobId)).path("state").asText();
            if ("RUNNING".equals(state)) {
                return;
            }
            if ("FAILED".equals(state) || "CANCELED".equals(state) || "FINISHED".equals(state)) {
                throw new IllegalStateException("Job terminal before savepoint: " + state
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
        for (int i = 0; i < 120; i++) {
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

    private String get(String url) throws Exception {
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("GET " + url + " -> " + resp.statusCode() + ": " + resp.body());
        }
        return resp.body();
    }

    private String safeGet(String url) {
        try {
            return get(url);
        } catch (Exception e) {
            return "(" + e.getMessage() + ")";
        }
    }

    private String post(String url, String json) throws Exception {
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(60))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json)).build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200 && resp.statusCode() != 202) {
            throw new IOException("POST " + url + " -> " + resp.statusCode() + ": " + resp.body());
        }
        return resp.body();
    }

    private static Path findJobJar() throws IOException {
        try (Stream<Path> s = Files.list(Paths.get("target"))) {
            return s.filter(p -> {
                        String n = p.getFileName().toString();
                        return n.startsWith("flink-savepoint-generator") && n.endsWith(".jar") && !n.contains("original");
                    })
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("job jar not found — run `mvn package`"));
        }
    }

    private static Path resolveOutputDir() throws IOException {
        Path out = Paths.get(System.getProperty("kafka.savepoint.dir", "../tests/fixtures/savepoint-kafka"))
                .toAbsolutePath().normalize();
        if (Files.exists(out)) {
            try (Stream<Path> w = Files.walk(out)) {
                w.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.delete(p); } catch (IOException e) { throw new RuntimeException(e); }
                });
            }
        }
        Files.createDirectories(out);
        return out;
    }

    private void copySavepointFromContainer(String dirInContainer, Path dest) throws Exception {
        var ls = JOBMANAGER.execInContainer("sh", "-c", "ls -1 " + dirInContainer);
        if (ls.getExitCode() != 0) {
            throw new IllegalStateException("ls " + dirInContainer + ": " + ls.getStderr());
        }
        for (String name : ls.getStdout().split("\\R")) {
            if (!name.isBlank()) {
                JOBMANAGER.copyFileFromContainer(dirInContainer + "/" + name, dest.resolve(name).toString());
            }
        }
    }
}
