package com.test.explorer;

import com.test.explorer.model.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

/**
 * Process function that populates every supported state type for testing.
 *
 * State types covered:
 * - ValueState<Long> (simple Long)
 * - ValueState<String> (simple String)
 * - ValueState<Boolean> (simple Boolean)
 * - ValueState<Date> (java.util.Date)
 * - ValueState<SimpleEvent> (POJO with primitives + Enum + Date)
 * - ValueState<ComplexRecord> (POJO with nested POJO + all date/time types + arrays)
 * - MapState<String, SimpleEvent> (Map with String key, POJO value)
 * - MapState<Long, Date> (Map with Long key, Date value)
 * - MapState<EventType, Boolean> (Map with Enum key, Boolean value)
 * - ListState<String> (List of strings)
 * - ListState<SimpleEvent> (List of POJOs)
 */
public class AllTypesProcessFunction
        extends KeyedProcessFunction<String, String, String> {

    // VALUE states
    private transient ValueState<Long> counterState;
    private transient ValueState<String> labelState;
    private transient ValueState<Boolean> activeState;
    private transient ValueState<Date> lastSeenState;
    private transient ValueState<SimpleEvent> simpleEventState;
    private transient ValueState<ComplexRecord> complexRecordState;

    // MAP states
    private transient MapState<String, SimpleEvent> eventsByType;
    private transient MapState<Long, Date> timestampIndex;
    private transient MapState<EventType, Boolean> flagsByCategory;

    // LIST states
    private transient ListState<String> logEntries;
    private transient ListState<SimpleEvent> eventHistory;
    private transient ListState<Long> largeList;

    @Override
    public void open(Configuration parameters) throws Exception {
        // VALUE states
        counterState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("counter", Long.class));
        labelState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("label", String.class));
        activeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("active", Boolean.class));
        lastSeenState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-seen", Date.class));
        simpleEventState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("simple-event",
                        TypeInformation.of(SimpleEvent.class)));
        complexRecordState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("complex-record",
                        TypeInformation.of(ComplexRecord.class)));

        // MAP states
        eventsByType = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("events-by-type",
                        Types.STRING,
                        TypeInformation.of(SimpleEvent.class)));
        timestampIndex = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("timestamp-index",
                        Types.LONG, TypeInformation.of(Date.class)));
        flagsByCategory = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("flags-by-category",
                        TypeInformation.of(EventType.class), Types.BOOLEAN));

        // LIST states
        logEntries = getRuntimeContext().getListState(
                new ListStateDescriptor<>("log-entries", String.class));
        eventHistory = getRuntimeContext().getListState(
                new ListStateDescriptor<>("event-history",
                        TypeInformation.of(SimpleEvent.class)));
        largeList = getRuntimeContext().getListState(
                new ListStateDescriptor<>("large-list", Long.class));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out)
            throws Exception {
        String key = ctx.getCurrentKey();
        long now = System.currentTimeMillis();

        // Populate VALUE states
        Long count = counterState.value();
        counterState.update(count == null ? 1L : count + 1);

        labelState.update("label-for-" + key);
        activeState.update(key.hashCode() % 2 == 0);
        lastSeenState.update(new Date(now));

        SimpleEvent se = new SimpleEvent(
                key + "-evt-" + count,
                now,
                true,
                new Date(now - 86400000L), // yesterday
                EventType.values()[Math.abs(key.hashCode()) % EventType.values().length]
        );
        simpleEventState.update(se);

        ComplexRecord cr = new ComplexRecord();
        cr.name = "record-" + key;
        cr.count = count == null ? 1 : count.intValue() + 1;
        cr.score = 3.14159 * cr.count;
        cr.verified = true;
        cr.legacyDate = new Date(now);
        cr.localDateTime = LocalDateTime.of(2024, 3, 15, 14, 30, 0);
        cr.localDate = LocalDate.of(2024, 6, 21);
        cr.localTime = LocalTime.of(9, 15, 30);
        cr.instant = Instant.ofEpochSecond(now / 1000, 123456789);
        cr.address = new NestedAddress("123 Main St", "TestCity", 75001);
        cr.tags = new String[]{"alpha", "beta", "gamma"};
        cr.payload = new byte[]{0x48, 0x65, 0x6C, 0x6C, 0x6F}; // "Hello"
        cr.status = EventType.UPDATE;
        complexRecordState.update(cr);

        // Populate MAP states
        for (EventType et : EventType.values()) {
            eventsByType.put(et.name(), new SimpleEvent(
                    key + "-" + et.name(), now, true, new Date(now), et));
        }
        timestampIndex.put(now, new Date(now));
        timestampIndex.put(now - 3600000L, new Date(now - 3600000L));
        for (EventType et : EventType.values()) {
            flagsByCategory.put(et, et.ordinal() % 2 == 0);
        }

        // Populate LIST states
        logEntries.add("Entry at " + new Date(now));
        logEntries.add("Key=" + key + " count=" + count);

        eventHistory.add(se);

        // Large list — 600 elements (>500) to exercise the TUI entry scrollbar
        for (int i = 0; i < 600; i++) {
            largeList.add(now + i * 1000L);
        }

        out.collect(key + ":processed:" + count);
    }
}
