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

import com.google.gson.Gson;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Servlet extends HttpServlet {

    private final static Logger LOGGER = Logger.getLogger(Servlet.class.getName());

    private static void sleepBasedOnRequestHeader(final Request request, final String headerName, final String label) {
        final String sleepMsString = request.getHeader(headerName);
        if (sleepMsString != null) {
            try {
                LOGGER.info("sleeping " + sleepMsString + " ms before sending " + label);
                Thread.sleep(Long.parseLong(sleepMsString));
            } catch (final Exception ex) {
                LOGGER.log(Level.SEVERE, "error while sleeping before sending " + label, ex);
            }
        }
    }

    private static String retrieveCorrelationId(final HttpServletRequest request) {
        final String correlationId = request.getHeader("x-cid");
        if (correlationId != null) {
            return correlationId;
        } else {
            return UUID.randomUUID().toString();
        }
    }

    static long slurpRequest(final InputStream inputStream) throws IOException {
        long totalBytesRead = 0;
        final byte[] buffer = new byte[32768];

        while (true) {
            final int bytesRead = inputStream.read(buffer);
            if (bytesRead == -1) {
                LOGGER.info("total bytes read = " + totalBytesRead);
                break;
            }
            totalBytesRead += bytesRead;
        }

        return totalBytesRead;
    }

    @Override
    protected void service(final HttpServletRequest request, final HttpServletResponse response)
        throws IOException, ServletException {

        final String correlationId = retrieveCorrelationId(request);
        LOGGER.info(correlationId + " received " + request.getProtocol() + " " + request.getMethod() +
            " request at " + request.getRequestURI());
        response.setHeader("x-cid", correlationId);
        response.setHeader("server", "sediment:" + HttpConfiguration.SERVER_VERSION);

        final Request jettyRequest = (Request) request;
        final Response jettyResponse = (Response) response;

        switch (request.getRequestURI()) {
            case "/status": {
                handleStatusRequest(jettyRequest, jettyResponse);
                break;
            }
            case "/trailers": {
                handleTrailersRequest(jettyRequest, jettyResponse);
                break;
            }
            default: {
                LOGGER.severe(correlationId + " unsupported endpoint: " + request.getRequestURI());
                super.service(request, response);
                break;
            }
        }
        LOGGER.info(correlationId + " done processing request");
    }

    private void handleStatusRequest(final Request request, final Response response) throws IOException {
        final String contentParam = request.getParameter("content");
        final String responseContent = contentParam == null ? "OK" : contentParam;
        response.setContentType("text/plain");
        response.getWriter().write(responseContent);
    }

    private void handleTrailersRequest(final Request request, final Response response) throws IOException {
        final RequestInfo requestInfo = populateRequestInfo(request);
        final String responseBody = new Gson().toJson(requestInfo);
        final byte[] outputBytes = responseBody.getBytes();

        final HttpFields trailer = createResponseTrailers(request);
        response.setTrailers(() -> {
            sleepBasedOnRequestHeader(
                request, "x-sediment-sleep-before-response-trailer-ms", "response trailers");
            return trailer;
        });

        final String responseStatusString = request.getHeader("x-sediment-response-status");
        if (responseStatusString == null) {
            response.setStatus(200);
        } else {
            response.setStatus(Integer.parseInt(responseStatusString));
        }

        final ByteArrayInputStream respInputStream = new ByteArrayInputStream(outputBytes);
        final ServletOutputStream respOutputStream = response.getOutputStream();
        while (true) {
            final byte[] tempBytes = new byte[256];
            final int bytesRead = respInputStream.read(tempBytes);
            if (bytesRead == -1) {
                break;
            }
            respOutputStream.write(tempBytes, 0, bytesRead);
            respOutputStream.flush();
            sleepBasedOnRequestHeader(
                request, "x-sediment-sleep-after-chunk-send-ms", "next chunk");
        }
    }

    private static HttpFields createResponseTrailers(final Request request) {
        final HttpFields trailerFields = new HttpFields();
        final String trailerPrefix = "x-sediment-response-trailer-";
        for (String name : Collections.list(request.getHeaderNames())) {
            if (name.toLowerCase().startsWith(trailerPrefix)) {
                final String trailerName = name.substring(trailerPrefix.length());
                trailerFields.add(trailerName, request.getHeader(name));
            }
        }
        return trailerFields;
    }

    private static RequestInfo populateRequestInfo(final Request request) throws IOException {

        final long bodyLength = slurpRequest(request.getInputStream());

        final Map<String, String> headers = new HashMap<>();
        for (final String name : Collections.list(request.getHeaderNames())) {
            final List<String> values = Collections.list(request.getHeaders(name));
            final String value = values.size() == 1 ? values.get(0) : String.join(", ", values);
            headers.put(name, value);
        }

        final Map<String, String> params = new HashMap<>();
        for (final String name : Collections.list(request.getParameterNames())) {
            final String[] values = request.getParameterValues(name);
            final String value = values.length == 1 ? values[0] : String.join(", ", values);
            params.put(name, value);
        }

        final Map<String, String> trailers = new HashMap<>();
        final HttpFields requestTrailers = request.getTrailers();
        if (requestTrailers != null) {
            for (final String name : requestTrailers.getFieldNamesCollection()) {
                final List<String> values = Collections.list(requestTrailers.getValues(name));
                final String value = values.size() == 1 ? values.get(0) : String.join(", ", values);
                trailers.put(name, value);
            }
        }

        final String userPrincipal = request.getUserPrincipal() != null ?
            request.getUserPrincipal().toString() : "";

        return new RequestInfo(
            bodyLength,
            request.getContentLengthLong(),
            request.getContextPath(),
            headers,
            request.getPathInfo(),
            request.getProtocol(),
            params,
            request.getMethod(),
            request.getRequestURI(),
            request.getScheme(),
            trailers,
            userPrincipal);
    }
}