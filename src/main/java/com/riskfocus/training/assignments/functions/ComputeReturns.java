package com.riskfocus.training.assignments.functions;

import com.riskfocus.training.assignments.domain.PriceReturns;
import com.riskfocus.training.assignments.domain.TBillRate;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Iterator;

// TODO: enforce a window size of 2...
public class ComputeReturns extends ProcessWindowFunction<TBillRate, PriceReturns, String, GlobalWindow> {

    private transient ValueState<Boolean> skipFirst;

    @Override
    public void open(Configuration config) {
        // TODO: implementation left as an exercise
    }

    @Override
    public void process(String key, Context context, Iterable<TBillRate> rates, Collector<PriceReturns> out) throws IOException {
        // TODO: implementation left as an exercise
    }
}
