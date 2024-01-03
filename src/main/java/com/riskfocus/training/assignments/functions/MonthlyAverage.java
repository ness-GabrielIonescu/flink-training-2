package com.riskfocus.training.assignments.functions;

import com.riskfocus.training.assignments.domain.MonthlyAverageReturn;
import com.riskfocus.training.assignments.domain.PriceReturns;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MonthlyAverage extends KeyedProcessFunction<String, PriceReturns, MonthlyAverageReturn> {
    private transient ValueState<Tuple2<Float, Long>> sumPrice;

    public MonthlyAverage() {
    }

    @Override
    public void open(Configuration conf) {
        sumPrice = getRuntimeContext().getState(new ValueStateDescriptor<>("sumPrices",
                TypeInformation.of(new TypeHint<Tuple2<Float, Long>>() {
                })));
    }

    @Override
    public void processElement(PriceReturns priceReturns, Context ctx, Collector<MonthlyAverageReturn> out) throws Exception {
        Tuple2<Float, Long> sumOfPricesAndCount = sumPrice.value();
        Float amount = priceReturns.getAmount();
        if (sumOfPricesAndCount == null) {
            sumPrice.update(Tuple2.of(amount, 1L));
        } else {
            sumPrice.update(Tuple2.of(sumOfPricesAndCount.f0 + amount, ++sumOfPricesAndCount.f1));
        }
        if (priceReturns.isEndOfMonth()) {
            Tuple2<Float, Long> finalSumAndCount = sumPrice.value();
            out.collect(new MonthlyAverageReturn(ctx.getCurrentKey(), finalSumAndCount.f0 / finalSumAndCount.f1));
            sumPrice.clear();
        }
    }
}
