package no.entra.jurfak;

import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;

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
        QueryResult result = app.readFromInflux("luft_inn", "SQ4101");
        log.debug("result: {}", result);

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
