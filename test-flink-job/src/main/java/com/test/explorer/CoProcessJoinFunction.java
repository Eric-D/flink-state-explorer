package com.test.explorer;

import com.test.explorer.model.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Tests KeyedCoProcessFunction — two connected keyed streams.
 * Both streams share the same keyed state backend.
 */
public class CoProcessJoinFunction
        extends KeyedCoProcessFunction<String, String, String, String> {

    private transient ValueState<String> leftValue;
    private transient ValueState<String> rightValue;
    private transient ValueState<Date> joinTimestamp;
    private transient MapState<String, Boolean> seenKeys;

    @Override
    public void open(Configuration parameters) throws Exception {
        leftValue = getRuntimeContext().getState(
                new ValueStateDescriptor<>("left-value", String.class));
        rightValue = getRuntimeContext().getState(
                new ValueStateDescriptor<>("right-value", String.class));
        joinTimestamp = getRuntimeContext().getState(
                new ValueStateDescriptor<>("join-timestamp",
                        TypeInformation.of(Date.class)));
        seenKeys = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("seen-keys", Types.STRING, Types.BOOLEAN));
    }

    @Override
    public void processElement1(String value, Context ctx, Collector<String> out)
            throws Exception {
        leftValue.update(value);
        seenKeys.put("left-" + value, true);
        joinTimestamp.update(new Date());

        String right = rightValue.value();
        if (right != null) {
            out.collect(ctx.getCurrentKey() + ":joined:" + value + "+" + right);
        }
    }

    @Override
    public void processElement2(String value, Context ctx, Collector<String> out)
            throws Exception {
        rightValue.update(value);
        seenKeys.put("right-" + value, true);
        joinTimestamp.update(new Date());

        String left = leftValue.value();
        if (left != null) {
            out.collect(ctx.getCurrentKey() + ":joined:" + left + "+" + value);
        }
    }
}
