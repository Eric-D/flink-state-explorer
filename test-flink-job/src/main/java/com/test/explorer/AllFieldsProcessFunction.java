package com.test.explorer;

import com.test.explorer.model.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

/**
 * Populates an AllFieldsPojo with every field type filled in.
 * Tests exhaustive POJO field deserialization.
 */
public class AllFieldsProcessFunction
        extends KeyedProcessFunction<String, String, String> {

    private transient ValueState<AllFieldsPojo> fullPojoState;
    private transient MapState<String, AllFieldsPojo> pojoMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        fullPojoState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("all-fields-pojo",
                        TypeInformation.of(AllFieldsPojo.class)));
        pojoMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("all-fields-map",
                        Types.STRING,
                        TypeInformation.of(AllFieldsPojo.class)));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out)
            throws Exception {
        AllFieldsPojo p = buildPojo(value);
        fullPojoState.update(p);
        pojoMapState.put("entry-" + value, p);

        out.collect(value + ":all-fields-done");
    }

    private AllFieldsPojo buildPojo(String key) {
        AllFieldsPojo p = new AllFieldsPojo();
        long now = System.currentTimeMillis();

        // Primitives
        p.boolField = true;
        p.byteField = (byte) 42;
        p.shortField = (short) 1234;
        p.intField = 999_999;
        p.longField = 123_456_789_012L;
        p.floatField = 3.14f;
        p.doubleField = 2.718281828;

        // Boxed (some null to test nullable)
        p.boolBoxed = false;
        p.byteBoxed = (byte) -1;
        p.shortBoxed = null; // intentionally null
        p.intBoxed = 42;
        p.longBoxed = now;
        p.floatBoxed = null; // intentionally null
        p.doubleBoxed = 99.99;

        // Strings
        p.stringField = "hello-" + key;
        p.nullString = null; // intentionally null

        // Char
        p.charField = 'A';
        p.charBoxed = 'Z';

        // Enum
        p.enumField = EventType.UPDATE;

        // Date/Time
        p.dateField = new Date(now);
        p.localDateTimeField = LocalDateTime.of(2024, 3, 15, 14, 30, 45, 123_000_000);
        p.localDateField = LocalDate.of(2024, 6, 21);
        p.localTimeField = LocalTime.of(9, 15, 30, 500_000_000);
        p.instantField = Instant.ofEpochSecond(now / 1000, 987_654_321);
        p.sqlTimestamp = new java.sql.Timestamp(now);

        // Primitive arrays
        p.byteArray = new byte[]{0x48, 0x65, 0x6C, 0x6C, 0x6F}; // "Hello"
        p.intArray = new int[]{10, 20, 30, 40, 50};
        p.longArray = new long[]{100L, 200L, now};
        p.doubleArray = new double[]{1.1, 2.2, 3.3};
        p.floatArray = new float[]{0.1f, 0.2f, 0.3f};
        p.boolArray = new boolean[]{true, false, true, true};
        p.shortArray = new short[]{1, 2, 3};
        p.charArray = new char[]{'X', 'Y', 'Z'};

        // Object arrays
        p.stringArray = new String[]{"alpha", "beta", "gamma", "delta"};

        // Collections
        p.stringList = Arrays.asList("list-a", "list-b", "list-c");
        p.intList = Arrays.asList(1, 2, 3, 4, 5);
        p.pojoList = Arrays.asList(
                new SimpleEvent("evt-1", now, true, new Date(now), EventType.CREATE),
                new SimpleEvent("evt-2", now, false, new Date(now - 3600000), EventType.DELETE)
        );

        // Maps
        Map<String, String> sm = new HashMap<>();
        sm.put("color", "blue");
        sm.put("size", "large");
        p.stringMap = sm;

        Map<String, Integer> sim = new HashMap<>();
        sim.put("score", 95);
        sim.put("level", 3);
        p.stringIntMap = sim;

        Map<Integer, String> ism = new HashMap<>();
        ism.put(1, "first");
        ism.put(2, "second");
        p.intStringMap = ism;

        // Nested POJO
        p.nestedPojo = new NestedAddress("123 Main St", "Paris", 75001);
        p.nullNested = null; // intentionally null

        // Deeply nested
        p.deepNested = new AllFieldsPojo.DeepNested(
                "deep-label",
                new NestedAddress("456 Oak Ave", "Lyon", 69001),
                EventType.ARCHIVE,
                Arrays.asList("deep-a", "deep-b")
        );

        return p;
    }
}
