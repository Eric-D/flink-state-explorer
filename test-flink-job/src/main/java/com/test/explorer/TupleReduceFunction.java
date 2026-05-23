package com.test.explorer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Tests ReducingState and Tuple types.
 * Tuple2/Tuple3 are very common in Flink and use TupleSerializer.
 */
public class TupleReduceFunction
        extends KeyedProcessFunction<String, String, String> {

    private transient ReducingState<Long> sumState;
    private transient org.apache.flink.api.common.state.ValueState<Tuple2<String, Long>> tupleState;
    private transient org.apache.flink.api.common.state.ValueState<Tuple3<String, Integer, Boolean>> triple;

    @Override
    public void open(Configuration parameters) throws Exception {
        sumState = getRuntimeContext().getReducingState(
                new ReducingStateDescriptor<>("running-sum", Long::sum, Types.LONG));

        tupleState = getRuntimeContext().getState(
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                        "tuple2-state",
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(
                                new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Long>>() {})));

        triple = getRuntimeContext().getState(
                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                        "tuple3-state",
                        org.apache.flink.api.common.typeinfo.TypeInformation.of(
                                new org.apache.flink.api.common.typeinfo.TypeHint<Tuple3<String, Integer, Boolean>>() {})));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out)
            throws Exception {
        sumState.add(1L);
        tupleState.update(Tuple2.of(value, System.currentTimeMillis()));
        triple.update(Tuple3.of(value, value.length(), value.contains("-")));
        out.collect(ctx.getCurrentKey() + ":sum:" + sumState.get());
    }
}
