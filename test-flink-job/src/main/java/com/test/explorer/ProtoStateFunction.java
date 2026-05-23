package com.test.explorer;

import com.test.explorer.model.ProtoWrapper;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Stores a protobuf message inside a {@link ProtoWrapper} POJO ({className, protoBytes}). This is
 * the typical pattern where Flink state carries an opaque protobuf payload that the explorer can
 * decode given a profile. Keyed over many distinct keys so the savepoint also exercises the TUI's
 * key-list scrollbar.
 *
 * <p>The bytes are hand-encoded protobuf wire format (no protoc dependency) for:
 * <pre>message TestEvent { int64 id = 1; string name = 2; bool active = 3; }  // com.test.proto.TestEvent</pre>
 */
public class ProtoStateFunction extends KeyedProcessFunction<String, String, String> {

    static final String PROTO_CLASS = "com.test.proto.TestEvent";

    private transient ValueState<ProtoWrapper> protoState;

    @Override
    public void open(Configuration parameters) {
        protoState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("proto-event",
                        TypeInformation.of(ProtoWrapper.class)));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        long id = Math.abs((long) value.hashCode());
        byte[] bytes = encodeTestEvent(id, "evt-" + value, (id & 1) == 0);
        protoState.update(new ProtoWrapper(PROTO_CLASS, bytes));
        out.collect(value);
    }

    /** Hand-encode protobuf wire format: int64 id=1, string name=2, bool active=3. */
    static byte[] encodeTestEvent(long id, String name, boolean active) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(0x08);                       // field 1 (id), wire type 0 = varint
        writeVarint(o, id);
        byte[] n = name.getBytes(StandardCharsets.UTF_8);
        o.write(0x12);                       // field 2 (name), wire type 2 = length-delimited
        writeVarint(o, n.length);
        o.write(n, 0, n.length);
        o.write(0x18);                       // field 3 (active), wire type 0 = varint
        o.write(active ? 1 : 0);
        return o.toByteArray();
    }

    private static void writeVarint(ByteArrayOutputStream o, long v) {
        while ((v & ~0x7FL) != 0) {
            o.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        o.write((int) (v & 0x7F));
    }
}
