package no.entra.jurfak;

import no.entra.jurfak.mappers.Derivative;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.InfluxDBResultMapper;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.List;

import static java.lang.StrictMath.abs;
import static no.cantara.config.ServiceConfig.getProperty;
import static org.slf4j.LoggerFactory.getLogger;

public class QueryToCsv {
    private static final Logger log = getLogger(QueryToCsv.class);

    private final InfluxDbConnector influxDbConnector;
    private final String measurementsName;
    private final String databaseName;

    public QueryToCsv(String measurementName) {
        this.measurementsName = measurementName;
        this.databaseName = "default_db";
        influxDbConnector = new InfluxDbConnector(measurementsName, databaseName);
        influxDbConnector.openDb();
    }

    public static void main(String[] args) throws IOException {
        String measurementName = getProperty("MEASUREMENT_NAME");
        QueryToCsv app = new QueryToCsv(measurementName);
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy hh:mm");
        String room = "SQ4101";
        QueryResult result = app.readFromInflux("luft_inn", room);
        log.debug("result: {}", result);
        InfluxDBResultMapper resultMapper = new InfluxDBResultMapper(); // thread-safe - can be reused
        List<Derivative> derivativeList = resultMapper.toPOJO(result, Derivative.class);
        boolean aboveTreshold = false;
        Instant aboveAtTime = null;
        for (Derivative derivative : derivativeList) {
            if (abs(derivative.getDerivative()) > 1) {
                aboveTreshold = true;
                aboveAtTime = derivative.getTime();
                break;
            }
        }
        if (aboveTreshold) {
            log.info("{} above treshold at {}", room, aboveAtTime);
        }
        app.closeDb();

    }

    private void closeDb() {
        influxDbConnector.closeDb();
    }


    QueryResult readFromInflux(String field, String room) {
        String select = "SELECT DERIVATIVE(\"luft_inn\")  FROM \"default_db\".\"innemiljo\".\"innemiljo_jurfak\" WHERE time > '2020-03-04T06:18:00.000Z' AND time < '2020-03-04T17:18:00.000Z' AND \"room\"='SK4202' ";
        Query query = new Query(select, databaseName); //new Query("SELECT \"" + field + "\" FROM \"" + measurementsName + "\" where", "default_db");
        return influxDbConnector.query(query);
    }
}
