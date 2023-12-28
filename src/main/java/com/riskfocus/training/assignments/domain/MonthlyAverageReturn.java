package com.riskfocus.training.assignments.domain;

import java.util.Objects;

public class MonthlyAverageReturn {
    private String date;
    private Float amount;

    public MonthlyAverageReturn(String date, Float amount) {
        this.date = date;
        this.amount = amount;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MonthlyAverageReturn that = (MonthlyAverageReturn) o;
        return Objects.equals(date, that.date) &&
                Objects.equals(amount, that.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, amount);
    }

    @Override
    public String toString() {
        return "TBillReturns{" +
                "yearMonth='" + date + '\'' +
                ", amount=" + amount +
                '}';
    }

    public Float getAmount() {
        return amount;
    }

    public void setAmount(Float amount) {
        this.amount = amount;
    }
}
