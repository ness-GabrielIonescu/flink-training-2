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

import com.riskfocus.training.assignments.domain.PriceReturns;
import com.riskfocus.training.assignments.domain.TBillRate;
import com.riskfocus.training.assignments.sinks.TestSink;
import com.riskfocus.training.assignments.sources.TestRateSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TBillReturnTest extends TestBase<TBillRate, PriceReturns> {

    private static final String[] args = new String[]{"2020-03",TBillPriceAssignment.SINK_RETURNS};

    private static final Testable TEST_APPLICATION = () -> TBillPriceAssignment.main(args);

    @Test
    public void testReturnsFromResources() throws Exception {
        assertReturns(testResults.resultMap.get(args[0]).returns, getResults(null));
    }

    @Test
    public void testReturns() throws Exception {
        TBillRate rate1 = new TBillRate();
        LocalDateTime localDateTime = LocalDateTime.of(2020,3,1,0,1);
        rate1.setQuoteTime(localDateTime);
        rate1.setClose(3.17);
        rate1.setEndOfMonth(Boolean.FALSE);
        TBillRate rate2 = new TBillRate();
        LocalDateTime localDateTime2 = LocalDateTime.of(2020,3,2,0,1);
        rate2.setQuoteTime(localDateTime2);
        rate2.setClose(3.15);
        rate2.setEndOfMonth(Boolean.FALSE);
        TestRateSource source = new TestRateSource(
                rate1, rate2
        );
        PriceReturns priceReturns = new PriceReturns(localDateTime2, (float) Math.log(rate2.getClosingPrice() / rate1.getClosingPrice()), Boolean.FALSE);
        List<PriceReturns> expected = Arrays.asList(priceReturns);
        assertReturns(expected, getResults(source));
    }

    private void assertReturns(List<PriceReturns> expected, List<PriceReturns> results) {
        int expectedSize = expected != null ? expected.size(): 0;
        int resultsSize = results != null ? results.size(): 0;
        assertEquals(expectedSize, resultsSize);
        for (int i=0; i<expectedSize; i++) {
            PriceReturns expectedPriceReturns = expected.get(i);
            PriceReturns resultPriceReturns = results.get(i);
            assertEquals(expectedPriceReturns.getDate(), resultPriceReturns.getDate());
            assertEquals(expectedPriceReturns.getAmount(), resultPriceReturns.getAmount(), 0.00001);
            assertEquals(expectedPriceReturns.isEndOfMonth(), resultPriceReturns.isEndOfMonth());
        }
    }

    protected List<PriceReturns> getResults(TestRateSource source) throws Exception {
        return runApp(source, new TestSink<>(), TEST_APPLICATION);
    }

}
