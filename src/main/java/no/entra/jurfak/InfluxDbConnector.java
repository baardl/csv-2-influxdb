package no.entra.jurfak;

import no.cantara.config.ServiceConfig;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;

import java.util.Properties;

import static no.cantara.config.ServiceConfig.getProperty;
import static org.slf4j.LoggerFactory.getLogger;

public class InfluxDbConnector {
    private static final Logger log = getLogger(InfluxDbConnector.class);

    private final String measurementsName;
    private final String databaseName;
    private InfluxDB influxDB = null;

    public InfluxDbConnector(String measurementsName, String databaseName) {
        this.measurementsName = measurementsName;
        this.databaseName = databaseName;
    }

    /*
    Used only for testing
     */
    protected InfluxDbConnector(String measurementsName, String databaseName, InfluxDB influxDB) {
        this.measurementsName = measurementsName;
        this.databaseName = databaseName;
        this.influxDB = influxDB;
    }

    void openDb() {
        Properties serviceConfig = ServiceConfig.loadProperties();
        String influxDbUrl = getProperty("INFLUXDB_URL");
        String influxDbUsername = getProperty("INFLUXDB_USERNAME");
        String influxDbPassword = getProperty("INFLUXDB_PASSWORD");
        influxDB = InfluxDBFactory.connect(influxDbUrl, influxDbUsername, influxDbPassword);
        String dbName = "default_db";
        influxDB.setDatabase(dbName);
        String rpName = getProperty("INFLUXDB_RETENTION_POLICY_NAME");
        influxDB.setRetentionPolicy(rpName);

        influxDB.enableBatch(BatchOptions.DEFAULTS.exceptionHandler(
                (failedPoints, throwable) -> {
                    for (Point failedPoint : failedPoints) {
                        log.trace("Failed point: {}", failedPoint);
                    }
                    log.error("error: {}", throwable.getMessage());
                    closeDb();
                })
        );
    }

    void closeDb() {
        if (influxDB != null) {
            influxDB.close();
        }
    }

    public InfluxDB getInfluxDB() {
        return influxDB;
    }

    public QueryResult query(Query query) {
        return influxDB.query(query);
    }
}
