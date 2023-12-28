package com.riskfocus.training.assignments.functions;

import com.riskfocus.training.assignments.domain.MonthlyAverageReturn;
import com.riskfocus.training.assignments.domain.PriceReturns;
import com.riskfocus.training.assignments.tbillprices.TBillPriceAssignment;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import com.riskfocus.training.assignments.domain.TBillRate;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.YearMonth;

public class MonthlyAverage extends KeyedProcessFunction<String, PriceReturns, MonthlyAverageReturn> {

    private static final Logger LOG = LoggerFactory.getLogger(MonthlyAverage.class);

    // Keyed, managed state, with an entry for each window, keyed by the window's end time.
    // There is a separate MapState object for each driver.
    //private transient MapState<String, Float> sumOfPrices;
    private transient ValueState<Tuple2<Float, Long>> sumPrice;

    public MonthlyAverage() {

    }

    @Override
    public void open(Configuration conf) throws Exception {
        // implementation left as an exercise
    }

    @Override
    public void processElement(PriceReturns priceReturns, Context ctx, Collector<MonthlyAverageReturn> out) throws Exception {
        // implementation left as an exercise
    }
}
