package com.test.explorer.model;

import java.io.Serializable;

/**
 * The common "opaque protobuf payload carried in a Flink POJO" pattern: a class-name string plus
 * the raw protobuf bytes. flink-explorer decodes {@code protoBytes} against a .proto schema when a
 * profile with a matching proto pattern (class_field=className, bytes_field=protoBytes) is loaded.
 *
 * <p>Public fields + no-arg constructor so Flink uses its PojoSerializer (not Kryo).
 */
public class ProtoWrapper implements Serializable {
    public String className;
    public byte[] protoBytes;

    public ProtoWrapper() {}

    public ProtoWrapper(String className, byte[] protoBytes) {
        this.className = className;
        this.protoBytes = protoBytes;
    }
}
