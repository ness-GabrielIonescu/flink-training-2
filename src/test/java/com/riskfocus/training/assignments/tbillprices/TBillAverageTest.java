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
import com.riskfocus.training.assignments.domain.TBillRate;
import com.riskfocus.training.assignments.domain.TBillTestRecord;
import com.riskfocus.training.assignments.sinks.TestSink;
import com.riskfocus.training.assignments.sources.TestRateSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TBillAverageTest extends TestBase<TBillRate, MonthlyAverageReturn> {

    private static final String[] args = new String[]{"2020-03",TBillPriceAssignment.SINK_AVERAGE};
    private static final Testable TEST_APPLICATION = () -> TBillPriceAssignment.main(args);

    @Test
    public void testAverage() throws Exception {
        List<MonthlyAverageReturn> results = getResults(null);
        assertNotNull(results);
        assertEquals(1, results.size());
        MonthlyAverageReturn result = results.get(0);
        TBillTestRecord expectedRecord = testResults.resultMap.get(args[0]);
        assertEquals(args[0], result.getDate());
        assertEquals(expectedRecord.average, result.getAmount(), 0.000001);
    }

    protected List<MonthlyAverageReturn> getResults(TestRateSource source) throws Exception {
        return runApp(source, new TestSink<>(), TEST_APPLICATION);
    }

}
