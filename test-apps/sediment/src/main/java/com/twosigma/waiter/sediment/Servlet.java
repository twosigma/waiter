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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
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
        final Map<String, Object> infoMap = populateRequestInfo(request);
        final String responseBody = new Gson().toJson(infoMap);
        final byte[] outputBytes = responseBody.getBytes();

        final HttpFields trailer = createResponseTrailers(request);
        if (trailer != null) {
            response.setTrailers(() -> {
                sleepBasedOnRequestHeader(
                    request, "x-sediment-sleep-before-response-trailer-ms", "response trailers");
                return trailer;
            });
        }

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
        if (trailerFields.size() == 0) {
            return null;
        } else {
            return trailerFields;
        }
    }

    private static Map<String, Object> convertFieldsToMap(final Collection<String> names, final Function<String, Object> retrieveValue) {
        final Map<String, Object> result = new HashMap<>();
        for (String name : names) {
            result.put(name, retrieveValue.apply(name));
        }
        return result;
    }

    private static Map<String, Object> populateRequestInfo(final Request request) throws IOException {

        // read body so it is safe to read trailers
        final long bodyLength = slurpRequest(request.getInputStream());

        final Map<String, Object> info = new HashMap<>();

        info.put("body-length", String.valueOf(bodyLength));
        if (request.getContentLengthLong() >= 0) {
            info.put("content-length", String.valueOf(request.getContentLengthLong()));
        }
        info.put("context-path", request.getContextPath());
        info.put("headers",
            convertFieldsToMap(
                Collections.list(request.getHeaderNames()),
                (name) -> {
                    final List<String> headerValues = Collections.list(request.getHeaders(name));
                    if (headerValues.size() == 1) {
                        return headerValues.get(0);
                    } else {
                        return headerValues;
                    }
                }));
        info.put("path-info", request.getPathInfo());
        info.put("protocol", request.getProtocol());
        info.put("method", request.getMethod());
        info.put("query-parameters",
            convertFieldsToMap(
                Collections.list(request.getParameterNames()),
                (name) -> {
                    final List<String> headerValues = Arrays.asList(request.getParameterValues(name));
                    if (headerValues.size() == 1) {
                        return headerValues.get(0);
                    } else {
                        return headerValues;
                    }
                }
            ));
        info.put("request-uri", request.getRequestURI());
        info.put("scheme", request.getScheme());
        final HttpFields requestTrailers = request.getTrailers();
        if (requestTrailers != null) {
            info.put("trailers",
                convertFieldsToMap(
                    requestTrailers.getFieldNamesCollection(),
                    (name) -> {
                        final List<String> trailerValues = Collections.list(requestTrailers.getValues(name));
                        if (trailerValues.size() == 1) {
                            return trailerValues.get(0);
                        } else {
                            return trailerValues;
                        }
                    }));
        }
        info.put("user-principal", request.getUserPrincipal() != null ? request.getUserPrincipal().toString() : "");

        return info;
    }
}