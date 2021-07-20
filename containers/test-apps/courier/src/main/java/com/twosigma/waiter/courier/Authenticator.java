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
package com.twosigma.waiter.courier;

import com.sun.net.httpserver.Headers;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.logging.Logger;

public interface Authenticator {

    boolean authenticate(Headers requestHeaders);

    class BasicAuthenticator implements Authenticator {

        private final static Logger LOGGER = Logger.getLogger(BasicAuthenticator.class.getName());

        private final String expectedAuthorization;

        public BasicAuthenticator(String username, String password) {
            String auth = username + ":" + password;
            byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
            expectedAuthorization = "Basic " + new String(encodedAuth);
        }

        @Override
        public boolean authenticate(Headers requestHeaders) {
            final String authorization = requestHeaders.getFirst("authorization");
            if (authorization == null) {
                LOGGER.info("No authorization header provided");
                return false;
            }
            if (!expectedAuthorization.equals(authorization)) {
                LOGGER.info("Invalid authorization header: " + authorization);
                return false;
            }
            LOGGER.info("Successfully authenticated request");
            return true;
        }
    }

    class DisabledAuthenticator implements Authenticator {

        @Override
        public boolean authenticate(Headers requestHeaders) {
            return true;
        }
    }
}
