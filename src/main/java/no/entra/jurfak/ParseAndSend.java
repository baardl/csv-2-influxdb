package no.entra.jurfak;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Hello world!
 */
public class ParseAndSend {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ParseAndSend.class);
    private static final String CSV_FILE_PATH = "./spjeld_ 4_etasje_Luftmengde.csv";

    private final String apiKey;
    private final String seriesName;

    public ParseAndSend(String apiKey, String seriesName) {
        this.apiKey = apiKey;
        this.seriesName = seriesName;
    }

    public static void main(String[] args) throws IOException {
       ParseAndSend app = new ParseAndSend("hei", "innemiljo");

        try (
                Reader reader = Files.newBufferedReader(Paths.get(CSV_FILE_PATH));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withIgnoreHeaderCase()
                        .withTrim());
        ) {
//        File csvData = new File("./spjeld_ 4_etasje_Luftmengde.csv");
//        Charset charset = Charset.forName("UTF-8");
//        CSVParser parser = CSVParser.parse(csvData, charset, CSVFormat.RFC4180);
            List<String> roomNames = csvParser.getHeaderNames();
            for (CSVRecord csvRecord : csvParser) {
                // Accessing values by Header names
                String timestamp = csvRecord.get("Timestamp");
                for (String roomName : roomNames) {
                    String luft = csvRecord.get(roomName);
                    app.sendToInflux("room", roomName, "luft", luft, timestamp);
                }
//                log.debug("Record {}", csvRecord);
            }
        }
        System.out.println("Hello World!");
    }

    void sendToInflux(String point, String pointValue, String field, String value, String timestamp) {
        log.trace(apiKey + "." + seriesName + "," + point + "=" + pointValue + "," + field + "=" + value + " " + timestamp);
    }
}
