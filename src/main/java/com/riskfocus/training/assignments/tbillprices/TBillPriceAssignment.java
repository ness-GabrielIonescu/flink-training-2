/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.riskfocus.training.assignments.tbillprices;

import com.riskfocus.training.assignments.domain.MonthlyAverageReturn;
import com.riskfocus.training.assignments.domain.PriceReturns;
import com.riskfocus.training.assignments.functions.ComputeReturns;
import com.riskfocus.training.assignments.functions.ComputeVolatility;
import com.riskfocus.training.assignments.functions.MonthlyAverage;
import com.riskfocus.training.assignments.utils.ExerciseBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.riskfocus.training.assignments.domain.TBillRate;
import com.riskfocus.training.assignments.sources.TBillRateSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;

public class TBillPriceAssignment extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(TBillPriceAssignment.class);
    public static final String SINK_PRICES = "PRICES";
    public static final String SINK_RETURNS = "RETURNS";
    public static final String SINK_AVERAGE = "AVERAGE";
    public static final String SINK_VOLATILITY = "VOLATILITY";

    public static String getKey(LocalDateTime dt) {
        return dt.format(DateTimeFormatter.ofPattern("yyyy-MM"));
    }

    public static boolean isIn(String value, String... keys) {
        return new HashSet<>(Arrays.asList(keys)).contains(value);
    }

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(ExerciseBase.parallelism);
        env.setParallelism(1);

        // read in the rates and group by year and month: e.g. 2020-04
        DataStream<TBillRate> rates = env.addSource(rateSourceOrTest(new TBillRateSource("/TBill_3M_Daily.csv")))
                .keyBy(TBillRate::getKey);
        // For unit tests we need the data only for a specific month
        DataStream<TBillRate> filteredRates = (args != null && args.length > 0) ?
                rates.filter(rate -> isIn(getKey(rate.getQuoteTime()), args)) : rates;
        DataStream<Tuple3<LocalDateTime, Double, Double>> prices = getPrices(filteredRates);

        String testSink = (args != null && args.length > 1) ? args[1] : null;
        
        if (SINK_PRICES.equals(testSink)) {
            prices.addSink(out);
        }

        DataStream<PriceReturns> returns = computeReturns(filteredRates);

        if (SINK_RETURNS.equals(testSink)) {
            returns.addSink(out);
        }

        DataStream<MonthlyAverageReturn> avgReturn = computeAverages(returns);
        if (SINK_AVERAGE.equals(testSink)) {
            avgReturn.addSink(out);
        }

        DataStream<Tuple2<String, Double>> volatility = computeVolatility(returns, avgReturn);
        if (SINK_VOLATILITY.equals(testSink)) {
            volatility.addSink(out);
        }
        volatility.print();

        // execute the transformation pipeline
        env.execute("TBillPrice");
    }

    public static DataStream<Tuple3<LocalDateTime, Double, Double>> getPrices(DataStream<TBillRate> rates) {
        return rates.<Tuple3<LocalDateTime, Double, Double>>map(new MapFunction<TBillRate, Tuple3<LocalDateTime, Double, Double>>() {
            @Override
            public Tuple3<LocalDateTime, Double, Double> map(TBillRate rate) throws Exception {
                return Tuple3.of(rate.getQuoteTime(), rate.getClose(), rate.getClosingPrice());
            }
        });
    }

    public static DataStream<PriceReturns> computeReturns(DataStream<TBillRate> rates) {
        return rates.keyBy(TBillRate::getKey)
                .countWindow(2, 1)
                .process(new ComputeReturns());

    }

    public static DataStream<MonthlyAverageReturn> computeAverages(DataStream<PriceReturns> priceReturns) {
        // Calculate a running average return for each month, return only the last element in the month (boolean flag is true)
        return priceReturns.keyBy(p -> getKey(p.getDate())).process(new MonthlyAverage());
    }

    public static DataStream<Tuple2<String, Double>> computeVolatility(DataStream<PriceReturns> priceReturns,
                                                                       DataStream<MonthlyAverageReturn> avgReturns) {

        return priceReturns.connect(avgReturns)
                .keyBy(priceReturns1 -> getKey(priceReturns1.getDate()), MonthlyAverageReturn::getDate)
                .flatMap(new ComputeVolatility());
    }

}
