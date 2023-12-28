package com.riskfocus.training.assignments.functions;

import com.riskfocus.training.assignments.tbillprices.TBillPriceAssignment;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import com.riskfocus.training.assignments.domain.MonthlyAverageReturn;
import com.riskfocus.training.assignments.domain.PriceReturns;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class ComputeVolatility extends RichCoFlatMapFunction<PriceReturns, MonthlyAverageReturn, Tuple2<String, Double>> {

    private ListState<PriceReturns> priceReturns;
    private ValueState<Float> returnAverage;
    private ValueState<Float> sum;

    @Override
    public void open(Configuration config) {
        // TODO: implementation left as an exercise
    }

    @Override
    public void flatMap1(PriceReturns value, Collector<Tuple2<String, Double>> out) throws Exception {
        // TODO: implementation left as an exercise
    }

    @Override
    public void flatMap2(MonthlyAverageReturn value, Collector<Tuple2<String, Double>> out) throws Exception {
        // TODO: implementation left as an exercise
    }

}
