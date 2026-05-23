package com.test.explorer.model;

import java.io.Serializable;

/**
 * Tests: nested POJO deserialization.
 */
public class NestedAddress implements Serializable {
    public String street;
    public String city;
    public int zipCode;

    public NestedAddress() {}

    public NestedAddress(String street, String city, int zipCode) {
        this.street = street;
        this.city = city;
        this.zipCode = zipCode;
    }
}
