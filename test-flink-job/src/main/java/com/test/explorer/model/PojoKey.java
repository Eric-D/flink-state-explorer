package com.test.explorer.model;

import java.io.Serializable;

/**
 * POJO used as keyBy key.
 * Tests: POJO key deserialization + namespace extraction for MapState.
 */
public class PojoKey implements Serializable {
    public String region;
    public String entityId;
    public EventType category;

    public PojoKey() {}

    public PojoKey(String region, String entityId, EventType category) {
        this.region = region;
        this.entityId = entityId;
        this.category = category;
    }

    @Override
    public int hashCode() {
        return (region + ":" + entityId + ":" + category).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PojoKey)) return false;
        PojoKey k = (PojoKey) o;
        return region.equals(k.region) && entityId.equals(k.entityId) && category == k.category;
    }
}
