package com.test.explorer;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.CountDownLatch;

/**
 * A deliberately LARGE but structurally simple job, used by {@code BigSavepointGeneratorIT} to
 * produce a savepoint of a few hundred MB so the explorer's read path can be measured at scale.
 *
 * <p>Unlike {@link ClusterJob} (every state type, a handful of keys) this holds one keyed operator
 * with N keys and three states per key:
 * <ul>
 *   <li>{@code big-counter}: {@code ValueState<Long>} = {@link #counterOf(int)}</li>
 *   <li>{@code big-label}:   {@code ValueState<String>} = {@link #labelOf(int)} (55-75 chars)</li>
 *   <li>{@code big-stats}:   {@code MapState<String,Long>} with 2 entries ({@code clicks},
 *       {@code views}) plus {@code buys} on even keys, i.e. 2-3 entries per key</li>
 * </ul>
 * Keys are {@code key-0000000 .. key-(N-1)} ({@link #keyOf(int)}), so every value is a pure
 * function of the key's index and can be asserted without a reference dump. Each key is emitted
 * exactly once; the sources then hold the job RUNNING until cancelled (a savepoint needs a running
 * job), the same latch pattern as {@link ClusterJob.HoldingSource}.
 *
 * <p>Parallelism is 2 and max parallelism is left at Flink's default (128). On disk each key costs
 * roughly 210 bytes across the three states, so the default {@value #DEFAULT_KEYS} keys give a
 * savepoint of about 200 MB. The first program argument overrides N.
 */
public class BigStateJob {

    public static final int DEFAULT_KEYS = 1_000_000;

    public static final String SOURCE_NAME = "Big Key Source";
    public static final String PROCESS_NAME = "Big State";
    public static final String COUNTER_STATE = "big-counter";
    public static final String LABEL_STATE = "big-label";
    public static final String STATS_STATE = "big-stats";

    private static final String FILLER =
            "the quick brown fox jumps over the lazy dog 0123456789";

    static final CountDownLatch HOLD_LATCH = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        int keys = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_KEYS;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().disableForceAvro();
        env.getConfig().disableForceKryo();

        env.addSource(new KeySource(keys))
                .name(SOURCE_NAME).uid("big-key-source")
                .keyBy(k -> k)
                .process(new BigStateFunction())
                .name(PROCESS_NAME).uid("big-state")
                .sinkTo(new DiscardingSink<>())
                .name("Big Sink").uid("big-sink");

        env.execute("Big Savepoint Generator (" + keys + " keys)");
    }

    // --- Deterministic key / value generators (shared with the IT and assertable from Rust) ----

    public static String keyOf(int i) {
        return String.format("key-%07d", i);
    }

    public static int indexOf(String key) {
        return Integer.parseInt(key.substring("key-".length()));
    }

    public static long counterOf(int i) {
        return i * 31L + 7;
    }

    /** 55-75 characters: {@code label:<key>:<hex mix>:<20-40 chars of filler>}. */
    public static String labelOf(int i) {
        long mixed = mix64(i);
        return "label:" + keyOf(i) + ":" + Long.toHexString(mixed) + ":"
                + FILLER.substring(0, 20 + (i % 21));
    }

    public static long clicksOf(int i) {
        return i % 1_000;
    }

    public static long viewsOf(int i) {
        return i % 7_919;
    }

    /** Present only on even indexes, so the map alternates between 2 and 3 entries. */
    public static boolean hasBuys(int i) {
        return (i & 1) == 0;
    }

    public static long buysOf(int i) {
        return i / 1_000;
    }

    /** splitmix64 finaliser: a cheap, well-spread 64-bit value per index. */
    private static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    // --- Operators ----------------------------------------------------------------------------

    /**
     * Emits {@code key-0000000 .. key-(N-1)} once, striped across the parallel instances, then
     * blocks until cancelled so the job stays RUNNING for the savepoint.
     */
    static final class KeySource extends RichParallelSourceFunction<String> {
        private final int keys;
        private volatile boolean running = true;

        KeySource(int keys) {
            this.keys = keys;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int subtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            int index = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            for (int i = index; i < keys && running; i += subtasks) {
                ctx.collect(keyOf(i));
            }
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

    /** Writes the three per-key states; emits nothing (the sink exists only to close the graph). */
    static final class BigStateFunction extends KeyedProcessFunction<String, String, String> {

        private transient ValueState<Long> counter;
        private transient ValueState<String> label;
        private transient MapState<String, Long> stats;

        @Override
        public void open(Configuration parameters) {
            counter = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(COUNTER_STATE, Types.LONG));
            label = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(LABEL_STATE, Types.STRING));
            stats = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(STATS_STATE, Types.STRING, Types.LONG));
        }

        @Override
        public void processElement(String key, Context ctx, Collector<String> out) throws Exception {
            int i = indexOf(key);
            counter.update(counterOf(i));
            label.update(labelOf(i));
            // String literals: one shared instance each, so the map keys cost no heap per key.
            stats.put("clicks", clicksOf(i));
            stats.put("views", viewsOf(i));
            if (hasBuys(i)) {
                stats.put("buys", buysOf(i));
            }
        }
    }
}
