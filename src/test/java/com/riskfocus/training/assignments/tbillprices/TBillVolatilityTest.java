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

import com.riskfocus.training.assignments.domain.TBillRate;
import com.riskfocus.training.assignments.domain.TBillTestRecord;
import com.riskfocus.training.assignments.sinks.TestSink;
import com.riskfocus.training.assignments.sources.TestRateSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TBillVolatilityTest extends TestBase<TBillRate, Tuple2<String, Double>> {

    private static final String[] args = new String[]{"2020-03",TBillPriceAssignment.SINK_VOLATILITY};
    private static final Testable TEST_APPLICATION = () -> TBillPriceAssignment.main(args);

    @Test
    public void testVolatility() throws Exception {
        List<Tuple2<String, Double>> results = getResults(null);
        assertNotNull(results);
        assertEquals(1, results.size());
        Tuple2<String, Double> result = results.get(0);
        TBillTestRecord expectedRecord = testResults.resultMap.get(args[0]);
        assertEquals(args[0], result.f0);
        assertEquals(expectedRecord.volatility, result.f1, 0.00001);
    }

    protected List<Tuple2<String, Double>> getResults(TestRateSource source) throws Exception {
        return runApp(source, new TestSink<>(), TEST_APPLICATION);
    }

}
