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

package com.riskfocus.training.assignments.domain;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * A TaxiFare is a taxi fare event.
 *
 * <p>A TaxiFare consists of
 * - the rideId of the event
 * - the time of the event
 */
public class TBillRate implements Serializable {

    private LocalDateTime quoteTime;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Boolean endOfMonth;

    public TBillRate() {

    }

    public TBillRate(LocalDateTime quoteTime, Double open, Double high, Double low, Double close, Boolean endOfMonth) {
        this.quoteTime = quoteTime;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.endOfMonth = endOfMonth;
    }

    public static TBillRate fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 6) {
            throw new RuntimeException("Invalid record: " + line);
        }

        TBillRate ride = new TBillRate();

        try {
            ride.quoteTime = LocalDate.parse(tokens[0]).atStartOfDay();
            ride.endOfMonth = tokens[1].length() > 0 && Boolean.parseBoolean(tokens[1]);
            ride.open = tokens[2].length() > 0 ? Double.parseDouble(tokens[2]) : 0.0;
            ride.high = tokens[3].length() > 0 ? Double.parseDouble(tokens[3]) : 0.0;
            ride.low = tokens[4].length() > 0 ? Double.parseDouble(tokens[4]) : 0.0;
            ride.close = tokens[5].length() > 0 ? Double.parseDouble(tokens[5]) : 0.0;

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    public static String getKey(LocalDateTime dt) {
        return dt.format(DateTimeFormatter.ofPattern("yyyy-MM"));
    }

    public long getEventTime() {
        return this.quoteTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public String getKey() {
        return quoteTime.format(DateTimeFormatter.ofPattern("yyyy-MM"));
    }

    public Double getClosingPrice() {
        // TODO: implementation of this method is left as an exercise
        return 0.0;
    }

    public boolean isEndOfMonth() {
        return endOfMonth;
    }

    public LocalDateTime getQuoteTime() {
        return quoteTime;
    }

    public void setQuoteTime(LocalDateTime quoteTime) {
        this.quoteTime = quoteTime;
    }

    public Double getOpen() {
        return open;
    }

    public void setOpen(Double open) {
        this.open = open;
    }

    public Double getHigh() {
        return high;
    }

    public void setHigh(Double high) {
        this.high = high;
    }

    public Double getLow() {
        return low;
    }

    public void setLow(Double low) {
        this.low = low;
    }

    public Double getClose() {
        return close;
    }

    public void setClose(Double close) {
        this.close = close;
    }

    public Boolean getEndOfMonth() {
        return endOfMonth;
    }

    public void setEndOfMonth(Boolean endOfMonth) {
        this.endOfMonth = endOfMonth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TBillRate tBillRate = (TBillRate) o;
        return Objects.equals(quoteTime, tBillRate.quoteTime) &&
                Objects.equals(open, tBillRate.open) &&
                Objects.equals(high, tBillRate.high) &&
                Objects.equals(low, tBillRate.low) &&
                Objects.equals(close, tBillRate.close) &&
                Objects.equals(endOfMonth, tBillRate.endOfMonth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(quoteTime, open, high, low, close, endOfMonth);
    }

    @Override
    public String toString() {
        return "TBillRate{" +
                "quoteTime=" + quoteTime +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", close=" + close +
                ", endOfMonth=" + endOfMonth +
                '}';
    }
}
