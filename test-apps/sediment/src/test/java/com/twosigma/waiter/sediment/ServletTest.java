package com.twosigma.waiter.sediment;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static junit.framework.TestCase.assertEquals;

/**
 * Unit test for simple App.
 */
public class ServletTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void testSlurpRequest() throws IOException {
        final byte[] dataBytes = new byte[12345];
        final ByteArrayInputStream dataStream = new ByteArrayInputStream(dataBytes);
        final long bytesRead = Servlet.slurpRequest(dataStream);
        assertEquals(dataBytes.length, bytesRead);
    }
}
