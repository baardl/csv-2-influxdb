package no.entra.jurfak;

import org.junit.Test;

import static no.entra.jurfak.ParseAndSend.findZone;
import static no.entra.jurfak.ParseAndSend.isEven;
import static org.junit.Assert.*;

public class ParseAndSendTest {

    @Test
    public void parseValue() {
        String millValue = "1 289,00";
        assertEquals(Double.valueOf("1289"), ParseAndSend.parseValue(millValue));
        assertEquals(Double.valueOf("1062"), ParseAndSend.parseValue("1 062,00"));
    }

    @Test
    public void findZoneTest() {
        assertEquals(1, findZone("1"));
        assertEquals(1, findZone("2"));
        assertEquals(2, findZone("3"));
        assertEquals(2, findZone("4"));
        assertEquals(3, findZone("5"));
    }

    @Test
    public void isEvenTest() {
        assertTrue(isEven(0));
        assertFalse(isEven(1));
        assertTrue(isEven(2));
    }
}