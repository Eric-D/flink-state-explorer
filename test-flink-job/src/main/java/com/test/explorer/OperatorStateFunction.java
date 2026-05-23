package com.test.explorer;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests non-keyed operator state via CheckpointedFunction.
 * Covers both operator-state distribution modes: ListState (SPLIT_DISTRIBUTE /
 * even-split) and UnionListState (UNION). BROADCAST mode is covered by the
 * broadcast-config state in BroadcastJoinFunction.
 */
public class OperatorStateFunction implements SinkFunction<String>, CheckpointedFunction {

    private transient ListState<String> operatorState;
    private transient ListState<String> unionState;
    private final List<String> buffer = new ArrayList<>();

    @Override
    public void invoke(String value, Context context) {
        buffer.add(value);
        // Keep buffer bounded
        if (buffer.size() > 20) {
            buffer.remove(0);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        operatorState.clear();
        unionState.clear();
        for (String s : buffer) {
            operatorState.add(s);
            unionState.add("union-" + s);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        operatorState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("operator-buffer", Types.STRING));
        unionState = context.getOperatorStateStore().getUnionListState(
                new ListStateDescriptor<>("operator-union-buffer", Types.STRING));
        if (context.isRestored()) {
            for (String s : operatorState.get()) {
                buffer.add(s);
            }
        }
    }
}
