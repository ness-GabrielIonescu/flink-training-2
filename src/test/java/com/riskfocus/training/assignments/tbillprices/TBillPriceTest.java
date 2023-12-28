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

import com.riskfocus.training.assignments.sources.TestRateSource;
import com.riskfocus.training.assignments.domain.TBillRate;
import com.riskfocus.training.assignments.sinks.TestSink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TBillPriceTest extends TestBase<TBillRate, Tuple3<LocalDateTime, Double, Double>> {

    private static final String[] args = new String[]{"2020-03",TBillPriceAssignment.SINK_PRICES};
    private static final Testable TEST_APPLICATION = () -> TBillPriceAssignment.main(args);

    @Test
    public void testPricesFromResources() throws Exception {
        assertPrices(testResults.resultMap.get(args[0]).prices, getResults(null));
    }

    @Test
    public void testPrices() throws Exception {
        TBillRate rate = new TBillRate();
        LocalDateTime localDateTime = LocalDateTime.of(2020,03,01,00,01);
        rate.setQuoteTime(localDateTime);
        rate.setClose(3.17);
        rate.setEndOfMonth(Boolean.FALSE);
        TestRateSource source = new TestRateSource(
                rate
        );
        Tuple3<LocalDateTime, Double, Double> tuple = Tuple3.of(localDateTime, rate.getClose(), rate.getClosingPrice());
        List<Tuple3<LocalDateTime, Double, Double>> expected = Arrays.asList(tuple);
        assertPrices(expected, getResults(source));
    }

    private void assertPrices(List<Tuple3<LocalDateTime, Double, Double>> expected, List<Tuple3<LocalDateTime, Double, Double>> results) {
        int expectedSize = expected != null ? expected.size(): 0;
        int resultsSize = results != null ? results.size(): 0;
        assertEquals(expectedSize, resultsSize);
        for (int i=0; i<expectedSize; i++) {
            Tuple3<LocalDateTime, Double, Double> expectedTuple = expected.get(i);
            Tuple3<LocalDateTime, Double, Double> resultTuple = results.get(i);
            assertEquals(expectedTuple.f0, resultTuple.f0);
            assertEquals(expectedTuple.f1, resultTuple.f1, 0.00001);
            assertEquals(expectedTuple.f2, resultTuple.f2, 0.00001);
        }
    }


    protected List<Tuple3<LocalDateTime, Double, Double>> getResults(TestRateSource source) throws Exception {
        return runApp(source, new TestSink<>(), TEST_APPLICATION);
    }

}
