package com.test.explorer;

import com.test.explorer.model.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Tests additional state and serializer types not covered by AllTypesProcessFunction.
 *
 * - AggregatingState
 * - ValueState<Short>, ValueState<Byte>, ValueState<Float>, ValueState<Double>
 * - ValueState<int[]>, ValueState<long[]>
 * - MapState<String, String> (simple)
 * - ValueState with null value (empty state)
 */
public class ExtraTypesProcessFunction
        extends KeyedProcessFunction<String, String, String> {

    // Numeric primitive types
    private transient ValueState<Short> shortState;
    private transient ValueState<Byte> byteState;
    private transient ValueState<Float> floatState;
    private transient ValueState<Double> doubleState;

    // Primitive arrays
    private transient ValueState<int[]> intArrayState;
    private transient ValueState<long[]> longArrayState;

    // byte[] and String[] as direct ValueState
    private transient ValueState<byte[]> byteArrayState;
    private transient ValueState<String[]> stringArrayState;

    // Simple map
    private transient MapState<String, String> simpleMap;

    // AggregatingState (average computation)
    private transient AggregatingState<Long, Double> averageState;

    // State that will remain null (never updated)
    private transient ValueState<String> nullableState;

    // TTL-enabled state: value serializer is wrapped in a TtlSerializer (TtlValue = ts + value)
    private transient ValueState<Long> ttlCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        shortState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("short-val", Short.class));
        byteState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("byte-val", Byte.class));
        floatState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("float-val", Float.class));
        doubleState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("double-val", Double.class));

        intArrayState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("int-array", int[].class));
        longArrayState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("long-array", long[].class));

        byteArrayState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("byte-array", byte[].class));
        stringArrayState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("string-array", String[].class));

        simpleMap = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("simple-string-map", Types.STRING, Types.STRING));

        averageState = getRuntimeContext().getAggregatingState(
                new AggregatingStateDescriptor<>("running-average",
                        new AverageAggregate(),
                        TypeInformation.of(double[].class)));

        nullableState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("nullable-never-set", String.class));

        ValueStateDescriptor<Long> ttlDesc =
                new ValueStateDescriptor<>("ttl-counter", Long.class);
        ttlDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1)).build());
        ttlCounter = getRuntimeContext().getState(ttlDesc);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out)
            throws Exception {
        shortState.update((short) (value.length() % 100));
        byteState.update((byte) (value.hashCode() % 128));
        floatState.update(3.14f * value.length());
        doubleState.update(2.71828 * value.hashCode());

        intArrayState.update(new int[]{1, 2, 3, value.length()});
        longArrayState.update(new long[]{100L, 200L, System.currentTimeMillis()});

        byteArrayState.update(new byte[]{0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x21}); // "Hello!"
        stringArrayState.update(new String[]{"tag-a", "tag-b", "tag-c"});

        simpleMap.put("key-a", "value-alpha");
        simpleMap.put("key-b", "value-beta");
        simpleMap.put("key-" + value, value.toUpperCase());

        averageState.add((long) value.length());

        Long ttl = ttlCounter.value();
        ttlCounter.update(ttl == null ? 1L : ttl + 1);

        // nullableState is intentionally never updated → remains null

        out.collect(value + ":extra-done");
    }

    /**
     * Aggregate function that computes running average.
     * Accumulator is the average (Double), input is Long.
     */
    static class AverageAggregate implements AggregateFunction<Long, double[], Double> {
        @Override
        public double[] createAccumulator() {
            return new double[]{0, 0}; // sum, count
        }

        @Override
        public double[] add(Long value, double[] acc) {
            acc[0] += value;
            acc[1] += 1;
            return acc;
        }

        @Override
        public Double getResult(double[] acc) {
            return acc[1] > 0 ? acc[0] / acc[1] : 0.0;
        }

        @Override
        public double[] merge(double[] a, double[] b) {
            a[0] += b[0];
            a[1] += b[1];
            return a;
        }
    }
}
