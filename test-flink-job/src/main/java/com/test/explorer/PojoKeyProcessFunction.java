package com.test.explorer;

import com.test.explorer.model.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * Tests POJO-keyed state.
 * The keyBy key is a PojoKey { region, entityId, category }.
 */
public class PojoKeyProcessFunction
        extends KeyedProcessFunction<PojoKey, String, String> {

    private transient ValueState<SimpleEvent> latestEvent;
    private transient MapState<String, Date> dateIndex;

    @Override
    public void open(Configuration parameters) throws Exception {
        latestEvent = getRuntimeContext().getState(
                new ValueStateDescriptor<>("latest-event",
                        TypeInformation.of(SimpleEvent.class)));
        dateIndex = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("date-index",
                        Types.STRING, TypeInformation.of(Date.class)));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out)
            throws Exception {
        PojoKey key = ctx.getCurrentKey();
        long now = System.currentTimeMillis();

        SimpleEvent se = new SimpleEvent(
                key.entityId, now, true, new Date(now), key.category);
        latestEvent.update(se);

        dateIndex.put("first-seen", new Date(now - 86400000L * 30));
        dateIndex.put("last-seen", new Date(now));

        out.collect(key.region + ":" + key.entityId + ":done");
    }
}
