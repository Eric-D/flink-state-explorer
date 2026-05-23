package com.test.explorer;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Tests timer state — registered timers are stored in the savepoint.
 * Both event-time and processing-time timers.
 */
public class TimerProcessFunction
        extends KeyedProcessFunction<String, String, String> {

    private transient ValueState<Long> lastTimerTs;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTimerTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-timer-ts", Types.LONG));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out)
            throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        // Register a processing-time timer 10s in the future
        long timerTs = now + 10_000L;
        ctx.timerService().registerProcessingTimeTimer(timerTs);
        lastTimerTs.update(timerTs);

        out.collect(value + ":timer-set:" + timerTs);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        out.collect(ctx.getCurrentKey() + ":timer-fired:" + timestamp);
    }
}
