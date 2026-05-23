package com.test.explorer;

import com.test.explorer.model.*;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.CountDownLatch;

/**
 * Flink job designed to run on a real cluster (not MiniCluster).
 * Populates all state types, then holds sources open until cancelled.
 * Trigger a savepoint via REST API or CLI, then cancel.
 */
public class ClusterJob {

    public static final MapStateDescriptor<String, String> BROADCAST_DESC =
            new MapStateDescriptor<>("broadcast-config", Types.STRING, Types.STRING);

    private static final String[] STRING_KEYS = {
            "user-alice", "user-bob", "user-charlie", "user-delta", "user-echo",
            "org-alpha", "org-beta", "org-gamma"
    };

    static final CountDownLatch HOLD_LATCH = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().disableForceAvro();
        env.getConfig().disableForceKryo();

        // --- Source ---
        DataStream<String> mainStream = env.addSource(new HoldingSource(STRING_KEYS))
                .name("Main Source").uid("main-source");

        // --- Operator 1: All Types (String key) ---
        mainStream.keyBy(v -> v)
                .process(new AllTypesProcessFunction())
                .name("All Types Processor").uid("all-types-string-key")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 1").uid("sink-1");

        // --- Operator 2: POJO key ---
        DataStream<String> pojoStream = env.addSource(new HoldingSource(
                new String[]{"EU:entity-001:CREATE", "EU:entity-002:UPDATE",
                        "US:entity-003:DELETE", "US:entity-004:ARCHIVE",
                        "AP:entity-005:CREATE"}))
                .name("Pojo Key Source").uid("pojo-key-source");

        pojoStream.keyBy(v -> {
                    String[] p = v.split(":");
                    return new PojoKey(p[0], p[1], EventType.valueOf(p[2]));
                }, TypeInformation.of(PojoKey.class))
                .process(new PojoKeyProcessFunction())
                .name("POJO Key Processor").uid("pojo-key-processor")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 2").uid("sink-2");

        // --- Operator 3: Long key ---
        mainStream.keyBy(v -> (long) Math.abs(v.hashCode()) % 100)
                .process(new SimpleCounterFunction())
                .name("Long Key Counter").uid("long-key-counter")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 3").uid("sink-3");

        // --- Operator 4: Broadcast ---
        DataStream<String> bcSource = env.addSource(new HoldingSource(
                new String[]{"config-a", "config-b"}))
                .name("Broadcast Source").uid("broadcast-source");
        BroadcastStream<String> bcStream = bcSource.broadcast(BROADCAST_DESC);

        mainStream.keyBy(v -> v)
                .connect(bcStream)
                .process(new BroadcastJoinFunction())
                .name("Broadcast Join").uid("broadcast-join")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 4").uid("sink-4");

        // --- Operator 5: Timer ---
        mainStream.keyBy(v -> v)
                .process(new TimerProcessFunction())
                .name("Timer Processor").uid("timer-processor")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 5").uid("sink-5");

        // --- Operator 6: Tuple + Reducing ---
        mainStream.keyBy(v -> v)
                .process(new TupleReduceFunction())
                .name("Tuple Reduce").uid("tuple-reduce")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 6").uid("sink-6");

        // --- Operator 7: Window ---
        mainStream.keyBy(v -> v)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .process(new WindowCountFunction())
                .name("Window Counter").uid("window-counter")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 7").uid("sink-7");

        // --- Operator 8: All Fields POJO ---
        mainStream.keyBy(v -> v)
                .process(new AllFieldsProcessFunction())
                .name("All Fields POJO").uid("all-fields-pojo")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 8").uid("sink-8");

        // --- Operator 9: Extra Types ---
        mainStream.keyBy(v -> v)
                .process(new ExtraTypesProcessFunction())
                .name("Extra Types").uid("extra-types")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 9").uid("sink-9");

        // --- Operator 10: CoProcess ---
        DataStream<String> left = env.addSource(new HoldingSource(
                new String[]{"left-1", "left-2", "left-3"}))
                .name("Left Source").uid("left-source");
        DataStream<String> right = env.addSource(new HoldingSource(
                new String[]{"right-1", "right-2"}))
                .name("Right Source").uid("right-source");

        left.keyBy(v -> v.split("-")[1])
                .connect(right.keyBy(v -> v.split("-")[1]))
                .process(new CoProcessJoinFunction())
                .name("CoProcess Join").uid("coprocess-join")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 10").uid("sink-10");

        // --- Operator 11: Integer key ---
        mainStream.keyBy(v -> (Integer) (Math.abs(v.hashCode()) % 50))
                .process(new IntKeyCounterFunction())
                .name("Integer Key Counter").uid("int-key-counter")
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 11").uid("sink-11");

        // --- Operator 12: Operator state ---
        mainStream.addSink(new OperatorStateFunction())
                .name("Operator State Sink").uid("operator-state-sink");

        // --- Operator 13: Protobuf-in-POJO + high cardinality (600 keys for scrollbar) ---
        String[] hcKeys = new String[600];
        for (int i = 0; i < hcKeys.length; i++) {
            hcKeys[i] = "hc-" + i;
        }
        // parallelism 1 so all 600 keys land in a single subtask (the explorer reads one
        // subtask's keyed state), giving a >500-key list to exercise the scrollbar.
        env.addSource(new HoldingSource(hcKeys))
                .name("HighCard Source").uid("highcard-source")
                .keyBy(v -> v)
                .process(new ProtoStateFunction())
                .name("Proto State (600 keys)").uid("proto-state").setParallelism(1)
                .sinkTo(new org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink<>())
                .name("Sink 13").uid("sink-13");

        env.execute("Savepoint Test Generator");
    }

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
            while (running) {
                try { HOLD_LATCH.await(); break; }
                catch (InterruptedException e) { break; }
            }
        }

        @Override
        public void cancel() {
            running = false;
            HOLD_LATCH.countDown();
        }
    }
}
