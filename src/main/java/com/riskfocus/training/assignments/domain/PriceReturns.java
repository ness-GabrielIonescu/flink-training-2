package com.riskfocus.training.assignments.domain;

import java.time.LocalDateTime;
import java.util.Objects;

public class PriceReturns {
    private LocalDateTime date;
    private Float amount;
    private boolean endOfMonth;

    public PriceReturns(LocalDateTime date, Float amount, boolean endOfMonth) {
        this.date = date;
        this.amount = amount;
        this.endOfMonth = endOfMonth;
    }

    public LocalDateTime getDate() {
        return date;
    }

    public void setDate(LocalDateTime date) {
        this.date = date;
    }

    public Float getAmount() {
        return amount;
    }

    public void setAmount(Float amount) {
        this.amount = amount;
    }

    public boolean isEndOfMonth() {
        return endOfMonth;
    }

    public void setEndOfMonth(boolean endOfMonth) {
        this.endOfMonth = endOfMonth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PriceReturns that = (PriceReturns) o;
        return endOfMonth == that.endOfMonth &&
                Objects.equals(date, that.date) &&
                Objects.equals(amount, that.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, amount, endOfMonth);
    }

    @Override
    public String toString() {
        return "PriceReturns{" +
                "date=" + date +
                ", amount=" + amount +
                ", endOfMonth=" + endOfMonth +
                '}';
    }
}
