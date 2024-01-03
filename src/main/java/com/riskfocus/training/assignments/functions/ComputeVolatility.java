package com.riskfocus.training.assignments.functions;

import com.riskfocus.training.assignments.domain.MonthlyAverageReturn;
import com.riskfocus.training.assignments.domain.PriceReturns;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class ComputeVolatility extends RichCoFlatMapFunction<PriceReturns, MonthlyAverageReturn, Tuple2<String, Double>> {

    private static final int FIXED_NUM_OF_DAYS_IN_MONTH = 21;
    private static final int MULTIPLY_COEF = 100;
    private static final int VOLATILITY_ANNUALIZED_MONTHLY_CONSTANT = 252;
    private transient ListState<PriceReturns> priceReturns;
    private transient ValueState<Float> returnAverage;

    @Override
    public void open(Configuration config) {
        priceReturns = getRuntimeContext().getListState(new ListStateDescriptor<>("priceReturns", PriceReturns.class));
        returnAverage = getRuntimeContext().getState(new ValueStateDescriptor<>("returnAverage", Float.class));
    }

    @Override
    public void flatMap1(PriceReturns currentPriceReturn, Collector<Tuple2<String, Double>> out) throws Exception {
        Float returnValue = returnAverage.value();
        priceReturns.add(currentPriceReturn);
        if (currentPriceReturn.isEndOfMonth() && returnValue != null) {
            outputResult(currentPriceReturn, out, returnValue);
        }
    }

    @Override
    public void flatMap2(MonthlyAverageReturn value, Collector<Tuple2<String, Double>> dateAndVolatility) throws Exception {
        Iterable<PriceReturns> priceReturnsIterable = priceReturns.get();
        Optional<PriceReturns> endOfMonthPriceResultOptional = getEndOfMonthPriceResult(priceReturnsIterable);
        if (endOfMonthPriceResultOptional.isPresent()) {
            outputResult(endOfMonthPriceResultOptional.get(), dateAndVolatility, value.getAmount());
        } else {
            returnAverage.update(value.getAmount());
        }
    }


    private void outputResult(PriceReturns endOfMonthPriceResult, Collector<Tuple2<String, Double>> out,
                              Float returnValue) throws Exception {
        double result = calculateAnnualizedMonthlyVolatility(returnValue);
        out.collect(Tuple2.of(toYYYYMM(endOfMonthPriceResult.getDate()), result));
        priceReturns.clear();
        returnAverage.clear();
    }

    private String toYYYYMM(LocalDateTime localDateTime) {
        return localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM"));
    }

    private double calculateAnnualizedMonthlyVolatility(Float returnValue) throws Exception {
        float sum = calculateSum(returnValue);
        return Math.sqrt(sum / (FIXED_NUM_OF_DAYS_IN_MONTH - 1)) * Math.sqrt(VOLATILITY_ANNUALIZED_MONTHLY_CONSTANT) * MULTIPLY_COEF;
    }

    private float calculateSum(Float returnValue) throws Exception {
        float sum = 0;
        for (PriceReturns priceReturnFromState : priceReturns.get()) {
            sum += (float) Math.pow(priceReturnFromState.getAmount() - returnValue, 2);
        }
        return sum;
    }

    private Optional<PriceReturns> getEndOfMonthPriceResult(Iterable<PriceReturns> priceReturnsIterable) {
        if (priceReturnsIterable == null) {
            return Optional.empty();
        }
        for (PriceReturns priceReturn : priceReturnsIterable) {
            if (priceReturn.isEndOfMonth()) {
                return Optional.of(priceReturn);
            }
        }
        return Optional.empty();
    }

}
