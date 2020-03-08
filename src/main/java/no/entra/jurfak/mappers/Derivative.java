package no.entra.jurfak.mappers;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;
import org.influxdb.annotation.TimeColumn;

import java.time.Instant;

@Measurement(name = "innemiljo_jurfak")
public class Derivative {

    @TimeColumn
    @Column(name = "time")
    private Instant time;
    @Column(name = "derivative")
    private Double derivative;


    public Instant getTime() {
        return time;
    }

    public Double getDerivative() {
        return derivative;
    }
}
