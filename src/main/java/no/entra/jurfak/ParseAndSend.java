package no.entra.jurfak;

import no.cantara.config.ServiceConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static no.cantara.config.ServiceConfig.getProperty;

/**
 * Hello world!
 */
public class ParseAndSend {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ParseAndSend.class);

    private final String measurementsName;
    private InfluxDB influxDB = null;


    public ParseAndSend(String measurementName) {
        this.measurementsName = measurementName;
    }

    public static void main(String[] args) throws IOException {
        ParseAndSend app = new ParseAndSend( "innemiljo_test");
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy hh:mm");
        app.openDb();
        QueryResult result = app.readFromInflux("test_data", "sample_random_number");
        log.debug("result: {}", result);
        populate(app, formatter);

        app.closeDb();

    }

    protected static void populate(ParseAndSend app, SimpleDateFormat formatter) throws IOException {
        String csvFilePath = ServiceConfig.getProperty("CSV_FILE_PATH");
        try (

                Reader reader = Files.newBufferedReader(Paths.get(csvFilePath));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withIgnoreHeaderCase()
                        .withTrim());
        ) {
            List<String> roomNames = csvParser.getHeaderNames();
            for (CSVRecord csvRecord : csvParser) {
                // Accessing values by Header names
                String timestamp = csvRecord.get("Timestamp");
                for (String roomName : roomNames) {
                    if (roomName.equals("Timestamp")) {
                        //do nothing
                    } else {
                        String luft = csvRecord.get(roomName);
                        if (luft != null && !luft.isEmpty()) {
                            try {
                                Date timestampDate = formatter.parse(timestamp);
                                Long timestampLong = timestampDate.getTime();
                                Double luftValue = parseValue(luft);
                                app.sendToInflux("room", roomName, "luft", luftValue, timestampLong * 1000000);
//                                app.sendToInflux("room", roomName, "luft", luftValue,System.currentTimeMillis());
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }

        }
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

//        influxDB.enableBatch(BatchOptions.DEFAULTS);
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

    QueryResult readFromInflux(String measurment, String field) {
        Query query = new Query("SELECT \"" + field + "\" FROM \"" + measurment + "\"", "default_db");
        return influxDB.query(query);
    }

    void sendToInflux(String tag, String tagValue, String field, Double value, long timestamp) throws ParseException {
        Point point = Point.measurement(measurementsName)
                .time(timestamp, TimeUnit.NANOSECONDS)
                .tag(tag, tagValue)
                .addField(field, value)
                .build();
        log.trace(measurementsName + "," + tag + "=" + tagValue + "," + field + "=" + value + " " + timestamp);
        influxDB.write(point);

    }

    void closeDb() {
        if (influxDB != null) {
            influxDB.close();
        }
    }

    static Double parseValue(String value) {
        String parsable = value.replace(" ", ""); //.replace(",", ".");
        parsable = parsable.replace(",", ".");
        return Double.parseDouble(parsable);
    }
}
