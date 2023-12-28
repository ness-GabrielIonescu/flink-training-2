package com.riskfocus.training.assignments.domain;

import org.apache.flink.api.java.tuple.Tuple3;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class TBillTestRecord {
    public List<Tuple3<LocalDateTime, Double, Double>> prices = new ArrayList<>();
    public List<PriceReturns> returns = new ArrayList<>();
    public Float average;
    public Double volatility;
}
