package com.test.explorer;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Simple function with Long key and ValueState<Long>.
 * Tests: Long key type, simple ValueState.
 */
public class SimpleCounterFunction
        extends KeyedProcessFunction<Long, String, String> {

    private transient ValueState<Long> count;

    @Override
    public void open(Configuration parameters) throws Exception {
        count = getRuntimeContext().getState(
                new ValueStateDescriptor<>("element-count", Long.class));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out)
            throws Exception {
        Long c = count.value();
        count.update(c == null ? 1L : c + 1);
        out.collect(ctx.getCurrentKey() + ":" + count.value());
    }
}
