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

package com.riskfocus.training.assignments.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.riskfocus.training.assignments.domain.TBillRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * This SourceFunction generates a data stream of TBillRate records which are
 * read from a csv input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 *
 * <code>
 * StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 * </code>
 */
public class TBillRateSource implements SourceFunction<TBillRate> {

    private static final Logger LOG = LoggerFactory.getLogger(TBillRateSource.class);

    private final String dataFilePath;

    /**
     * Serves the TbillRate records from the specified and ordered  input file.
     *
     * @param dataFilePath The input file from which the TBill Rate records are read.
     */
    public TBillRateSource(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    @Override
    public void run(SourceContext<TBillRate> sourceContext) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TBillRateSource.class.getResourceAsStream(dataFilePath)))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                TBillRate rate = TBillRate.fromString(line);
                sourceContext.collectWithTimestamp(rate, rate.getEventTime());
                LOG.debug("Added TBill {}, with timestamp {}", rate, rate.getEventTime());
            }
        } catch (Exception e) {
            LOG.error("Exception: {}", e);
        }
    }

    @Override
    public void cancel() {
        // do not need to implement
    }

}

