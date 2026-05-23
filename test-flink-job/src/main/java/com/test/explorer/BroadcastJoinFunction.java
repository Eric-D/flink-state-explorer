package com.test.explorer;

import com.test.explorer.model.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Tests KeyedBroadcastProcessFunction with BroadcastState + keyed state.
 * BroadcastState is a non-keyed MapState shared across all keys.
 */
public class BroadcastJoinFunction
        extends KeyedBroadcastProcessFunction<String, String, String, String> {

    // Keyed state (per-key)
    private transient ValueState<SimpleEvent> lastEvent;
    private transient MapState<String, Date> eventDates;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastEvent = getRuntimeContext().getState(
                new ValueStateDescriptor<>("broadcast-last-event",
                        TypeInformation.of(SimpleEvent.class)));
        eventDates = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("broadcast-event-dates",
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        TypeInformation.of(Date.class)));
    }

    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out)
            throws Exception {
        long now = System.currentTimeMillis();
        SimpleEvent se = new SimpleEvent(value, now, true, new Date(now), EventType.CREATE);
        lastEvent.update(se);
        eventDates.put(value, new Date(now));
        out.collect("processed:" + value);
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<String> out)
            throws Exception {
        // Update broadcast state (shared config)
        ctx.getBroadcastState(ClusterJob.BROADCAST_DESC).put(value, value.toUpperCase());
    }
}
