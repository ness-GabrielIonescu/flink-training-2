package com.riskfocus.training.assignments.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// NOTE: head and tail operations on an unbounded stream doesn't make sense normally
public class Tail<K, I> extends KeyedProcessFunction<K, I, I> {

    private transient ValueState<Boolean> skipFirst;

    public Tail() {
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Boolean> startDescriptor =
                new ValueStateDescriptor<>("skipFirst", Boolean.class);
        skipFirst = getRuntimeContext().getState(startDescriptor);
    }

    @Override
    public void processElement(I value, Context ctx, Collector<I> out) throws Exception {
        if (skipFirst.value() != null)
            out.collect(value);
        else
            skipFirst.update(true);
    }
}
