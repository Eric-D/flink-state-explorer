package com.test.explorer.model;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

/**
 * Complex POJO testing all supported date/time types and nested POJO.
 * Tests: LocalDateTime, LocalDate, LocalTime, Instant, Date, nested POJO,
 *        nullable fields, String[], byte[].
 */
public class ComplexRecord implements Serializable {
    public String name;
    public int count;
    public double score;
    public boolean verified;
    public Date legacyDate;
    public LocalDateTime localDateTime;
    public LocalDate localDate;
    public LocalTime localTime;
    public Instant instant;
    public NestedAddress address;
    public String[] tags;
    public byte[] payload;
    public EventType status;

    public ComplexRecord() {}
}
