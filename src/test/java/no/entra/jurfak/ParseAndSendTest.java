package no.entra.jurfak;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ParseAndSendTest {

    @Test
    public void parseValue() {
        String millValue = "1 289,00";
        assertEquals(Double.valueOf("1289"), ParseAndSend.parseValue(millValue));
        assertEquals(Double.valueOf("1062"), ParseAndSend.parseValue("1 062,00"));
    }

    @Test
    public void findZone() {
        assertEquals(1, ParseAndSend.findZone("1"));
        assertEquals(1, ParseAndSend.findZone("2"));
        assertEquals(2, ParseAndSend.findZone("3"));
        assertEquals(2, ParseAndSend.findZone("4"));
        assertEquals(3, ParseAndSend.findZone("5"));
    }
}