package com.test.explorer;

import com.test.explorer.model.*;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.util.concurrent.CountDownLatch;

/**
 * Generates a canonical Flink 1.20 savepoint with all supported state types.
 *
 * Usage:
 *   mvn compile exec:java
 *
 * Output:
 *   ./target/savepoint-test/savepoint-xxx
 *
 * The generated savepoint can be used as a test fixture for flink-explorer.
 */
public class SavepointGenerator {

    // Keys used to generate diverse test data
    private static final String[] STRING_KEYS = {
        "user-alice", "user-bob", "user-charlie", "user-delta", "user-echo",
        "org-alpha", "org-beta", "org-gamma"
    };

    private static final PojoKey[] POJO_KEYS = {
        new PojoKey("EU", "entity-001", EventType.CREATE),
        new PojoKey("EU", "entity-002", EventType.UPDATE),
        new PojoKey("US", "entity-003", EventType.DELETE),
        new PojoKey("US", "entity-004", EventType.ARCHIVE),
        new PojoKey("AP", "entity-005", EventType.CREATE),
    };

    // BroadcastState descriptor (shared between generator and function)
    public static final MapStateDescriptor<String, String> BROADCAST_DESC =
            new MapStateDescriptor<>("broadcast-config", Types.STRING, Types.STRING);

    // Latch to hold the source open until savepoint is taken
    private static final CountDownLatch HOLD_LATCH = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        String savepointDir = new File("target/savepoint-test").getAbsolutePath();
        System.out.println("Savepoint output directory: " + savepointDir);

        // Configure mini-cluster
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file://" + savepointDir.replace("\\", "/"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(2);
        // Use Flink's built-in POJO serializer — no Avro, no Kryo
        env.getConfig().disableForceAvro();
        env.getConfig().disableForceKryo();

        // --- Operator 1: String key, all types ---
        DataStream<String> stringKeyStream = env.addSource(new HoldingSource(STRING_KEYS))
                .name("String Key Source")
                .uid("string-key-source");

        stringKeyStream
                .keyBy(v -> v)
                .process(new AllTypesProcessFunction())
                .name("All Types Processor (String key)")
                .uid("all-types-string-key")
                .addSink(new DiscardSink<>())
                .name("Discard Sink 1")
                .uid("discard-sink-1");

        // --- Operator 2: POJO key ---
        DataStream<String> pojoKeyStream = env.addSource(new HoldingSource(
                new String[]{"EU:entity-001:CREATE", "EU:entity-002:UPDATE",
                             "US:entity-003:DELETE", "US:entity-004:ARCHIVE",
                             "AP:entity-005:CREATE"}))
                .name("Pojo Key Source")
                .uid("pojo-key-source");

        pojoKeyStream
                .keyBy(v -> {
                    String[] parts = v.split(":");
                    return new PojoKey(parts[0], parts[1], EventType.valueOf(parts[2]));
                }, TypeInformation.of(PojoKey.class))
                .process(new PojoKeyProcessFunction())
                .name("POJO Key Processor")
                .uid("pojo-key-processor")
                .addSink(new DiscardSink<>())
                .name("Discard Sink 2")
                .uid("discard-sink-2");

        // --- Operator 3: Long key (simple counter) ---
        stringKeyStream
                .keyBy(v -> (long) Math.abs(v.hashCode()) % 100)
                .process(new SimpleCounterFunction())
                .name("Long Key Counter")
                .uid("long-key-counter")
                .addSink(new DiscardSink<>())
                .name("Discard Sink 3")
                .uid("discard-sink-3");

        // --- Operator 4: KeyedBroadcastProcessFunction ---
        DataStream<String> broadcastSource = env.addSource(
                new HoldingSource(new String[]{"config-a", "config-b"}))
                .name("Broadcast Source")
                .uid("broadcast-source");
        BroadcastStream<String> broadcastStream = broadcastSource.broadcast(BROADCAST_DESC);

        stringKeyStream
                .keyBy(v -> v)
                .connect(broadcastStream)
                .process(new BroadcastJoinFunction())
                .name("Broadcast Join Processor")
                .uid("broadcast-join")
                .addSink(new DiscardSink<>())
                .name("Discard Sink 4")
                .uid("discard-sink-4");

        // --- Operator 5: Timer state ---
        stringKeyStream
                .keyBy(v -> v)
                .process(new TimerProcessFunction())
                .name("Timer Processor")
                .uid("timer-processor")
                .addSink(new DiscardSink<>())
                .name("Discard Sink 5")
                .uid("discard-sink-5");

        // --- Operator 6: ReducingState + Tuple types ---
        stringKeyStream
                .keyBy(v -> v)
                .process(new TupleReduceFunction())
                .name("Tuple Reduce Processor")
                .uid("tuple-reduce")
                .addSink(new DiscardSink<>())
                .name("Discard Sink 6")
                .uid("discard-sink-6");

        // --- Operator 7: Window state ---
        stringKeyStream
                .keyBy(v -> v)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new WindowCountFunction())
                .name("Window Counter")
                .uid("window-counter")
                .addSink(new DiscardSink<>())
                .name("Discard Sink 7")
                .uid("discard-sink-7");

        // --- Operator 8: ALL field types in a single POJO ---
        stringKeyStream
                .keyBy(v -> v)
                .process(new AllFieldsProcessFunction())
                .name("All Fields POJO Processor")
                .uid("all-fields-pojo")
                .addSink(new DiscardSink<>())
                .name("Discard Sink All Fields")
                .uid("discard-sink-all-fields");

        // --- Operator 9: Extra types (Short, Byte, Float, Double, int[], long[], AggregatingState) ---
        stringKeyStream
                .keyBy(v -> v)
                .process(new ExtraTypesProcessFunction())
                .name("Extra Types Processor")
                .uid("extra-types")
                .addSink(new DiscardSink<>())
                .name("Discard Sink 8")
                .uid("discard-sink-8");

        // --- Operator 10: KeyedCoProcessFunction (connected streams) ---
        DataStream<String> leftStream = env.addSource(
                new HoldingSource(new String[]{"left-1", "left-2", "left-3"}))
                .name("Left Source")
                .uid("left-source");
        DataStream<String> rightStream = env.addSource(
                new HoldingSource(new String[]{"right-1", "right-2"}))
                .name("Right Source")
                .uid("right-source");

        leftStream
                .keyBy(v -> v.split("-")[1])
                .connect(rightStream.keyBy(v -> v.split("-")[1]))
                .process(new CoProcessJoinFunction())
                .name("CoProcess Join")
                .uid("coprocess-join")
                .addSink(new DiscardSink<>())
                .name("Discard Sink CoProcess")
                .uid("discard-sink-coprocess");

        // --- Operator 11: Integer key ---
        stringKeyStream
                .keyBy(v -> (Integer)(Math.abs(v.hashCode()) % 50))
                .process(new IntKeyCounterFunction())
                .name("Integer Key Counter")
                .uid("int-key-counter")
                .addSink(new DiscardSink<>())
                .name("Discard Sink IntKey")
                .uid("discard-sink-intkey");

        // --- Operator 12: Non-keyed operator state (CheckpointedFunction) ---
        stringKeyStream
                .addSink(new OperatorStateFunction())
                .name("Operator State Sink")
                .uid("operator-state-sink");

        // Submit and take savepoint
        JobClient client = env.executeAsync("Savepoint Generator");

        System.out.println("Job submitted, waiting for RUNNING status...");
        // Wait for job to reach RUNNING state
        for (int i = 0; i < 60; i++) {
            try {
                org.apache.flink.api.common.JobStatus status = client.getJobStatus().get();
                System.out.println("  Job status: " + status);
                if (status == org.apache.flink.api.common.JobStatus.RUNNING) {
                    break;
                }
                if (status.isTerminalState()) {
                    System.err.println("ERROR: Job reached terminal state: " + status);
                    System.exit(1);
                }
            } catch (Exception e) {
                System.out.println("  Waiting... (" + e.getMessage() + ")");
            }
            Thread.sleep(1000);
        }

        // Extra wait for state to be populated
        System.out.println("Job is RUNNING, waiting for state to be populated...");
        Thread.sleep(3000);

        System.out.println("Triggering savepoint...");
        try {
            String savepointPath = client.triggerSavepoint(null,
                    org.apache.flink.core.execution.SavepointFormatType.CANONICAL).get();
            System.out.println("Savepoint created at: " + savepointPath);
        } catch (Exception e) {
            System.err.println("ERROR: Failed to trigger savepoint: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        // Release the source and cancel
        HOLD_LATCH.countDown();
        try {
            client.cancel().get();
        } catch (Exception e) {
            // Job may already be finishing
        }

        System.out.println("Done!");
    }

    /**
     * Source that emits all elements then holds until the latch is released.
     */
    static class HoldingSource implements SourceFunction<String> {
        private final String[] elements;
        private volatile boolean running = true;

        HoldingSource(String[] elements) {
            this.elements = elements;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (String element : elements) {
                ctx.collect(element);
            }
            // Hold the source open so the job keeps running
            while (running) {
                try {
                    HOLD_LATCH.await();
                    break;
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
            HOLD_LATCH.countDown();
        }
    }

    /**
     * Sink that discards all elements.
     */
    static class DiscardSink<T> implements org.apache.flink.streaming.api.functions.sink.SinkFunction<T> {
        @Override
        public void invoke(T value, Context context) {
            // discard
        }
    }
}
