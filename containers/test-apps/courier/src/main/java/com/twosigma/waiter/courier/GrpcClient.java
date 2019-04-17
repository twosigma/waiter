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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GrpcClient {

    private static Function<String, Void> logFunction = new Function<String, Void>() {
        @Override
        public Void apply(final String message) {
            System.out.println(message);
            return null;
        }
    };

    public static void setLogFunction(final Function<String, Void> logFunction) {
        GrpcClient.logFunction = logFunction;
    }

    private static ManagedChannel initializeChannel(final String host, final int port) {
        logFunction.apply("initializing plaintext client at " + host + ":" + port);
        return ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext()
            .build();
    }

    private static void shutdownChannel(final ManagedChannel channel) throws InterruptedException {
        logFunction.apply("shutting down channel");
        channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
        logFunction.apply("channel shutdown successfully");
    }

    private static Metadata attachRequestHeaders(final Map<String, Object> headers) {
        final Metadata headerMetadata = new Metadata();
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            final String key = entry.getKey();
            final String value = String.valueOf(entry.getValue());
            headerMetadata.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);
        }
        return headerMetadata;
    }

    private static Channel wrapResponseLogger(final ManagedChannel channel) {
        return ClientInterceptors.intercept(channel, new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
                                                                       final CallOptions callOptions,
                                                                       final Channel next) {
                return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

                    @Override
                    public void start(final Listener<RespT> responseListener,
                                      final Metadata headers) {
                        super.start(new SimpleForwardingClientCallListener<RespT>(responseListener) {
                            @Override
                            public void onHeaders(final Metadata headers) {
                                logFunction.apply("headers received from server:" + headers);
                                super.onHeaders(headers);
                            }

                            @Override
                            public void onClose(final Status status, final Metadata trailers) {
                                logFunction.apply("status received from server:" + status);
                                logFunction.apply("trailers received from server:" + trailers);
                                super.onClose(status, trailers);
                            }
                        }, headers);
                    }
                };
            }
        });
    }

    public static CourierReply sendPackage(final String host,
                                           final int port,
                                           final Map<String, Object> headers,
                                           final String id,
                                           final String from,
                                           final String message) throws InterruptedException {
        final ManagedChannel channel = initializeChannel(host, port);

        try {
            final Channel wrappedChannel = wrapResponseLogger(channel);
            final Metadata headerMetadata = attachRequestHeaders(headers);

            final CourierGrpc.CourierFutureStub rawStub = CourierGrpc.newFutureStub(wrappedChannel);
            final CourierGrpc.CourierFutureStub futureStub = MetadataUtils.attachHeaders(rawStub, headerMetadata);

            logFunction.apply("will try to send package from " + from + " ...");
            final CourierRequest request = CourierRequest
                .newBuilder()
                .setId(id)
                .setFrom(from)
                .setMessage(message)
                .build();
            final CourierReply response;
            try {
                response = futureStub.sendPackage(request).get();
            } catch (final StatusRuntimeException e) {
                logFunction.apply("RPC failed, status: " + e.getStatus());
                return null;
            } catch (final Exception e) {
                logFunction.apply("RPC failed, message: " + e.getMessage());
                return null;
            }
            logFunction.apply("received response CourierReply{" +
                "id=" + response.getId() + ", " +
                "response=" + response.getResponse() + ", " +
                "message.length=" + response.getMessage().length() + "}");
            logFunction.apply("messages equal = " + message.equals(response.getMessage()));
            return response;

        } finally {
            shutdownChannel(channel);
        }
    }

    public static CourierSummary collectPackages(final String host,
                                                 final int port,
                                                 final Map<String, Object> headers,
                                                 final String idPrefix,
                                                 final String from,
                                                 final List<String> messages,
                                                 final int interMessageSleepMs,
                                                 final boolean lockStepMode) throws InterruptedException {
        final ManagedChannel channel = initializeChannel(host, port);

        try {
            final Semaphore lockStep = new Semaphore(1);

            final Channel wrappedChannel = wrapResponseLogger(channel);
            final Metadata headerMetadata = attachRequestHeaders(headers);

            final CourierGrpc.CourierStub rawStub = CourierGrpc.newStub(wrappedChannel);
            final CourierGrpc.CourierStub futureStub = MetadataUtils.attachHeaders(rawStub, headerMetadata);

            logFunction.apply("will try to send package from " + from + " ...");

            final CompletableFuture<CourierSummary> responsePromise = new CompletableFuture<>();
            try {
                final StreamObserver<CourierRequest> collector =
                    futureStub.collectPackages(new StreamObserver<CourierSummary>() {

                        private long numMessages = 0;
                        private long totalLength = 0;

                        @Override
                        public void onNext(final CourierSummary response) {
                            logFunction.apply("received response CourierSummary{" +
                                "count=" + response.getNumMessages() + ", " +
                                "length=" + response.getTotalLength() + "}");
                            numMessages += response.getNumMessages();
                            totalLength += response.getTotalLength();
                            if (lockStepMode) {
                                logFunction.apply("releasing semaphore after receiving response");
                                lockStep.release();
                            }
                        }

                        @Override
                        public void onError(final Throwable throwable) {
                            logFunction.apply("error in collecting summaries " + throwable);
                            responsePromise.complete(null);
                        }

                        @Override
                        public void onCompleted() {
                            logFunction.apply("completed collecting summaries");
                            responsePromise.complete(CourierSummary
                                .newBuilder()
                                .setNumMessages(numMessages)
                                .setTotalLength(totalLength)
                                .build());
                        }
                    });

                for (int i = 0; i < messages.size(); i++) {
                    final String requestId = idPrefix + i;
                    if (lockStepMode) {
                        logFunction.apply("acquiring semaphore before sending request " + requestId);
                        lockStep.acquire();
                    }
                    final CourierRequest request = CourierRequest
                        .newBuilder()
                        .setId(requestId)
                        .setFrom(from)
                        .setMessage(messages.get(i))
                        .build();
                    logFunction.apply("sending message CourierRequest{" +
                        "id=" + request.getId() + ", " +
                        "from=" + request.getFrom() + ", " +
                        "message.length=" + request.getMessage().length() + "}");
                    collector.onNext(request);
                    Thread.sleep(interMessageSleepMs);
                }
                logFunction.apply("completed sending packages");
                collector.onCompleted();

                return responsePromise.get();
            } catch (final StatusRuntimeException e) {
                logFunction.apply("RPC failed, status: " + e.getStatus());
                return null;
            } catch (final Exception e) {
                logFunction.apply("RPC failed, message: " + e.getMessage());
                return null;
            }

        } finally {
            shutdownChannel(channel);
        }
    }

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(final String... args) throws Exception {
        /* Access a service running on the local machine on port 8080 */
        final String host = "localhost"; // "courier.localtest.me";
        final int port = 8080; // 9091;
        final HashMap<String, Object> headers = new HashMap<>();

        final String id = UUID.randomUUID().toString();
        final String user = "Jim";
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100_000; i++) {
            sb.append("a");
            if (i % 1000 == 0) {
                sb.append(".");
            }
        }
        final CourierReply courierReply = sendPackage(host, port, headers, id, user, sb.toString());
        logFunction.apply("sendPackage response = " + courierReply);

        final List<String> messages = IntStream.range(0, 100).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final CourierSummary courierSummary =
            collectPackages(host, port, headers, "id-", "Jim", messages, 10, true);
        logFunction.apply("collectPackages response = " + courierSummary);
    }
}
