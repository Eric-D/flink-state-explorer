package com.test.explorer.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Exhaustive POJO covering ALL primitive types, array types, collections,
 * date/time types, nested POJOs, and enums as POJO fields.
 *
 * This ensures the PojoSerializerSnapshot records every field type
 * and the PojoSerializer writes every serialization pattern.
 */
public class AllFieldsPojo implements Serializable {

    // === Primitive types ===
    public boolean boolField;
    public byte byteField;
    public short shortField;
    public int intField;
    public long longField;
    public float floatField;
    public double doubleField;

    // === Boxed types (nullable) ===
    public Boolean boolBoxed;
    public Byte byteBoxed;
    public Short shortBoxed;
    public Integer intBoxed;
    public Long longBoxed;
    public Float floatBoxed;
    public Double doubleBoxed;

    // === String ===
    public String stringField;
    public String nullString;  // intentionally left null

    // === Char ===
    public char charField;
    public Character charBoxed;

    // === Enum ===
    public EventType enumField;

    // === Date/Time types ===
    public Date dateField;
    public LocalDateTime localDateTimeField;
    public LocalDate localDateField;
    public LocalTime localTimeField;
    public Instant instantField;
    public Timestamp sqlTimestamp;

    // === Primitive arrays ===
    public byte[] byteArray;
    public int[] intArray;
    public long[] longArray;
    public double[] doubleArray;
    public float[] floatArray;
    public boolean[] boolArray;
    public short[] shortArray;
    public char[] charArray;

    // === Object arrays ===
    public String[] stringArray;

    // === Collections ===
    public List<String> stringList;
    public List<Integer> intList;
    public List<SimpleEvent> pojoList;

    // === Maps ===
    public Map<String, String> stringMap;
    public Map<String, Integer> stringIntMap;
    public Map<Integer, String> intStringMap;

    // === Nested POJO ===
    public NestedAddress nestedPojo;
    public NestedAddress nullNested;  // intentionally left null

    // === Deeply nested ===
    public DeepNested deepNested;

    public AllFieldsPojo() {}

    /**
     * Deeply nested POJO to test multi-level recursion.
     */
    public static class DeepNested implements Serializable {
        public String label;
        public NestedAddress innerAddress;
        public EventType innerEnum;
        public List<String> innerList;

        public DeepNested() {}

        public DeepNested(String label, NestedAddress innerAddress,
                          EventType innerEnum, List<String> innerList) {
            this.label = label;
            this.innerAddress = innerAddress;
            this.innerEnum = innerEnum;
            this.innerList = innerList;
        }
    }
}
