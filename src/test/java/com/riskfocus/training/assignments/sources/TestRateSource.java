package com.riskfocus.training.assignments.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import com.riskfocus.training.assignments.domain.TBillRate;

public class TestRateSource extends TestSource<TBillRate> implements ResultTypeQueryable<TBillRate> {
    public TestRateSource(Object... eventsOrWatermarks) {
        this.testStream = eventsOrWatermarks;
    }

    @Override
    long getTimestamp(TBillRate ride) {
        return ride.getEventTime();
    }

    @Override
    public TypeInformation<TBillRate> getProducedType() {
        return TypeInformation.of(TBillRate.class);
    }
}
