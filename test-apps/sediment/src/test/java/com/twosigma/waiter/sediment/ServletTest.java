/*
 * Copyright (c) Two Sigma Open Source, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
