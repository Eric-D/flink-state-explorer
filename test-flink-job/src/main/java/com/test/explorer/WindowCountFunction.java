package com.test.explorer;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Tests ProcessWindowFunction — window state uses TimeWindow as namespace
 * instead of VoidNamespace.
 */
public class WindowCountFunction
        extends ProcessWindowFunction<String, String, String, TimeWindow> {

    @Override
    public void process(String key, Context ctx, Iterable<String> elements,
                        Collector<String> out) throws Exception {
        int count = 0;
        for (String e : elements) {
            count++;
        }
        out.collect(key + ":window:" + ctx.window().getStart() + ":" + count);
    }
}
