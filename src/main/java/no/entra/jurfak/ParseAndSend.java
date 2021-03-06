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
import java.util.*;
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
        String measurementName = getProperty("MEASUREMENT_NAME");
        ParseAndSend app = new ParseAndSend(measurementName);
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
            List<String> columnNames = csvParser.getHeaderNames();
            for (CSVRecord csvRecord : csvParser) {
                // Accessing values by Header names
                String timestamp = csvRecord.get("Timestamp");
                for (String columnName : columnNames) {
                    if (columnName.equals("Timestamp")) {
                        //do nothing
                    } else {
                        String columnValue = csvRecord.get(columnName);
                        //Expexting roomName-n where n = 1,2,3,4...
                        if (columnValue != null && !columnValue.isEmpty()) {
                            try {
                                String[] sensorNameParts = columnName.split("-");
                                String roomName = sensorNameParts[0];
                                String sensorNumberStr = sensorNameParts[1];
                                int zone = findZone(sensorNumberStr);

                                Date timestampDate = formatter.parse(timestamp);
                                Long timestampLong = timestampDate.getTime();
                                Double luftValue = parseValue(columnValue);

//Rom=sk4106, sone=1, luft_inn=23,5, spjell_inn=23,5
                                Map<String, String> tags = new HashMap<>();
                                tags.put("room", roomName);
                                tags.put("observation", "luft");
                                tags.put("zone", Integer.toString(zone));
                                String luftKey = "luft_inn";
                                if (isEven(Integer.valueOf(sensorNumberStr))) {
                                    luftKey = "luft_ut";
                                }
                                app.sendToInflux(tags, luftKey, luftValue, timestampLong * 1000000);
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }

        }
    }

    static int findZone(String sensorNumberStr) {
        int sensorNumber = Integer.valueOf(sensorNumberStr);
        int quotient = sensorNumber / 2;
        int remainer = sensorNumber % 2;
        int zone = quotient + remainer;
        return zone;
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

    QueryResult readFromInflux(String measurment, String field) {
        Query query = new Query("SELECT \"" + field + "\" FROM \"" + measurment + "\"", "default_db");
        return influxDB.query(query);
    }

    void sendToInflux(Map<String, String> tags, String field, Double value, long timestamp) throws ParseException {
        Point.Builder pointBuilder = Point.measurement(measurementsName)
                .time(timestamp, TimeUnit.NANOSECONDS)
                .addField(field, value);
        for (String key : tags.keySet()) {
            String tagValue = tags.get(key);
            pointBuilder = pointBuilder.tag(key, tagValue);
        }
        Point point = pointBuilder.build();
        log.trace(measurementsName + "," + tags + "," + field + "=" + value + " " + timestamp);
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

    static boolean isEven(int number) {
        return number % 2 == 0;
    }
}
