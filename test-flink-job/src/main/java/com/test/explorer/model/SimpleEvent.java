package com.test.explorer.model;

import java.io.Serializable;
import java.util.Date;

/**
 * Simple POJO with primitive-like fields.
 * Tests: String, Long, Boolean, Date, Enum deserialization.
 */
public class SimpleEvent implements Serializable {
    public String id;
    public long timestamp;
    public boolean active;
    public Date createdAt;
    public EventType eventType;

    public SimpleEvent() {}

    public SimpleEvent(String id, long timestamp, boolean active, Date createdAt, EventType eventType) {
        this.id = id;
        this.timestamp = timestamp;
        this.active = active;
        this.createdAt = createdAt;
        this.eventType = eventType;
    }
}
