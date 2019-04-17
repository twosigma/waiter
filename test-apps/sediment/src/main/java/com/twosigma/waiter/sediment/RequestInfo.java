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

import java.util.List;
import java.util.Map;

public class RequestInfo {

    private final long bodyLength;
    private final long contentLength;
    private final String contextPath;
    private final Map<String, String> headers;
    private final String pathInfo;
    private final String protocol;
    private final Map<String, String> queryParameters;
    private final String requestMethod;
    private final String requestUri;
    private final String scheme;
    private final Map<String, String> trailers;
    private final String userPrincipal;

    RequestInfo(
        final long bodyLength, final long contentLength, final String contextPath,
        final Map<String, String> headers, final String pathInfo,
        final String protocol, final Map<String, String> queryParameters,
        final String requestMethod, final String requestUri, final String scheme,
        final Map<String, String> trailers, final String userPrincipal) {

        this.bodyLength = bodyLength;
        this.contentLength = contentLength;
        this.contextPath = contextPath;
        this.headers = headers;
        this.pathInfo = pathInfo;
        this.protocol = protocol;
        this.queryParameters = queryParameters;
        this.requestMethod = requestMethod;
        this.requestUri = requestUri;
        this.scheme = scheme;
        this.trailers = trailers;
        this.userPrincipal = userPrincipal;
    }

    public long getBodyLength() {
        return bodyLength;
    }

    public long getContentLength() {
        return contentLength;
    }

    public String getContextPath() {
        return contextPath;
    }

    public String getPathInfo() {
        return pathInfo;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public String getRequestUri() {
        return requestUri;
    }

    public String getScheme() {
        return scheme;
    }

    public String getUserPrincipal() {
        return userPrincipal;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, String> getQueryParameters() {
        return queryParameters;
    }

    public Map<String, String> getTrailers() {
        return trailers;
    }
}
