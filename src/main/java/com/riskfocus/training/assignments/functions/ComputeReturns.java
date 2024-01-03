package com.riskfocus.training.assignments.functions;

import com.riskfocus.training.assignments.domain.PriceReturns;
import com.riskfocus.training.assignments.domain.TBillRate;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class ComputeReturns extends ProcessWindowFunction<TBillRate, PriceReturns, String, GlobalWindow> {
    @Override
    public void process(String key, Context context, Iterable<TBillRate> billRates, Collector<PriceReturns> out) {
        Iterator<TBillRate> billRateIterator = billRates.iterator();
        TBillRate currentBillRate = billRateIterator.next();
        if (!billRateIterator.hasNext()) {
            return;
        }
        TBillRate futureBillRate = billRateIterator.next();
        PriceReturns priceReturns = new PriceReturns(futureBillRate.getQuoteTime(),
                calculatePriceReturn(futureBillRate, currentBillRate), futureBillRate.getEndOfMonth());
        out.collect(priceReturns);
    }

    private float calculatePriceReturn(TBillRate futureBillRate, TBillRate currentBillRate) {
        return (float) Math.log(futureBillRate.getClosingPrice()
                / currentBillRate.getClosingPrice());
    }
}
