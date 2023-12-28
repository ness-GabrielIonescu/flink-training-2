package com.riskfocus.training.assignments.domain;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class TBillTestResults {

    private static final Logger LOG = LoggerFactory.getLogger(TBillTestResults.class);

    public Map<String, TBillTestRecord> resultMap = new HashMap<>();

    public  static TBillTestResults ofResource(String resource) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TBillTestResults.class.getResourceAsStream(resource)))) {
            TBillTestResults results = new TBillTestResults();
            String line = null;
            while ((line = reader.readLine()) != null) {
                results.parseString(line);
            }
            return results;
        } catch (Exception e) {
            LOG.error("Exception: {}", e);
        }
        return null;
    }

    private void parseString(String line) {

        String[] tokens = line.split(",",-1);
        if (tokens.length != 7) {
            throw new RuntimeException("Invalid record: " + line);
        }

        LocalDate localDate = LocalDate.parse(tokens[0]);
        LocalDateTime localDateTime = localDate.atStartOfDay();
        String key = TBillRate.getKey(localDateTime);

        resultMap.computeIfAbsent(key, k -> new TBillTestRecord());

        resultMap.computeIfPresent(key, (k, r)-> {
            try {
                Boolean isEndMonth = tokens[1].length() > 0 ? Boolean.parseBoolean(tokens[1]) : Boolean.FALSE;
                r.prices.add(Tuple3.of(localDateTime, tokens[2].length() > 0 ? Double.parseDouble(tokens[2]) : 0.0, tokens[3].length() > 0 ? Double.parseDouble(tokens[3]) : 0.0));
                // no return value on the first of the month
                String returnValue = tokens[4];
                if (!returnValue.isEmpty()) {
                    PriceReturns priceReturns = new PriceReturns(localDateTime, Float.parseFloat(returnValue), isEndMonth);
                    r.returns.add(priceReturns);
                }
                String average = tokens[5];
                if (!average.isEmpty()) {
                    r.average = Float.parseFloat(average);
                }
                String volatility = tokens[6];
                if (!volatility.isEmpty()) {
                    r.volatility = Double.parseDouble(volatility);
                }
                return r;
            } catch (NumberFormatException nfe) {
                throw new RuntimeException("Invalid record: " + line, nfe);
            }
        });
    }
}
