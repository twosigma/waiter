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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GrpcClient {

    private final static Logger LOGGER = Logger.getLogger(GrpcServer.class.getName());

    private static Function<String, Void> logFunction = new Function<String, Void>() {
        @Override
        public Void apply(final String message) {
            LOGGER.info(message);
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
        if (channel.isShutdown()) {
            logFunction.apply("channel shutdown successfully");
        } else {
            logFunction.apply("channel shutdown timed out!");
        }
    }

    private static Metadata createRequestHeadersMetadata(final Map<String, Object> headers) {
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
            final Metadata headerMetadata = createRequestHeadersMetadata(headers);

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
            } catch (ExecutionException e) {
                final Status status = Status.fromThrowable(e.getCause());
                logFunction.apply("RPC execution failed: " + status);
                return CourierReply
                    .newBuilder()
                    .setId(status.getCode().toString())
                    .setMessage(status.getDescription())
                    .setResponse("error")
                    .build();
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

    public static List<CourierSummary> collectPackages(final String host,
                                                       final int port,
                                                       final Map<String, Object> headers,
                                                       final List<String> ids,
                                                       final String from,
                                                       final List<String> messages,
                                                       final int interMessageSleepMs,
                                                       final boolean lockStepMode,
                                                       final int cancelThreshold) throws InterruptedException {
        final ManagedChannel channel = initializeChannel(host, port);
        final AtomicBoolean awaitChannelTermination = new AtomicBoolean(true);

        try {
            final Semaphore lockStep = new Semaphore(1);
            final AtomicBoolean errorSignal = new AtomicBoolean(false);

            final Channel wrappedChannel = wrapResponseLogger(channel);
            final Metadata headerMetadata = createRequestHeadersMetadata(headers);

            final CourierGrpc.CourierStub rawStub = CourierGrpc.newStub(wrappedChannel);
            final CourierGrpc.CourierStub futureStub = MetadataUtils.attachHeaders(rawStub, headerMetadata);

            logFunction.apply("will try to send package from " + from + " ...");

            final CompletableFuture<List<CourierSummary>> responsePromise = new CompletableFuture<>();
            try {
                final StreamObserver<CourierRequest> collector =
                    futureStub.collectPackages(new StreamObserver<CourierSummary>() {

                        final List<CourierSummary> resultList = new ArrayList<>();

                        @Override
                        public void onNext(final CourierSummary response) {
                            logFunction.apply("received response CourierSummary{" +
                                "count=" + response.getNumMessages() + ", " +
                                "length=" + response.getTotalLength() + "}");
                            resultList.add(response);
                            if (lockStepMode) {
                                logFunction.apply("releasing semaphore after receiving response");
                                lockStep.release();
                            }
                        }

                        @Override
                        public void onError(final Throwable throwable) {
                            logFunction.apply("error in collecting summaries " + throwable);
                            errorSignal.compareAndSet(false, true);
                            resolveResponsePromise();
                            if (lockStepMode) {
                                logFunction.apply("releasing semaphore after receiving error");
                                lockStep.release();
                            }
                            if (throwable instanceof StatusRuntimeException) {
                                final StatusRuntimeException exception = (StatusRuntimeException) throwable;
                                final CourierSummary response = CourierSummary
                                    .newBuilder()
                                    .setNumMessages(0)
                                    .setStatusCode(exception.getStatus().getCode().name())
                                    .setStatusDescription(exception.getStatus().getDescription())
                                    .build();
                                resultList.add(response);
                            }
                        }

                        @Override
                        public void onCompleted() {
                            logFunction.apply("completed collecting summaries");
                            resolveResponsePromise();
                        }

                        private void resolveResponsePromise() {
                            logFunction.apply("client result has " + resultList.size() + " entries");
                            responsePromise.complete(resultList);
                        }
                    });

                for (int i = 0; i < messages.size(); i++) {
                    if (i >= cancelThreshold) {
                        logFunction.apply("cancelling sending messages");
                        awaitChannelTermination.set(false);
                        channel.shutdownNow();
                        throw new CancellationException("Cancel threshold reached: " + cancelThreshold);
                    }
                    if (errorSignal.get()) {
                        logFunction.apply("aborting sending messages as error was discovered");
                        break;
                    }
                    final String requestId = ids.get(i);
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
            if (awaitChannelTermination.get()) {
                shutdownChannel(channel);
            }
        }
    }

    public static List<CourierSummary> collectPackages(final String host,
                                                       final int port,
                                                       final Map<String, Object> headers,
                                                       final String idPrefix,
                                                       final String from,
                                                       final List<String> messages,
                                                       final int interMessageSleepMs,
                                                       final boolean lockStepMode,
                                                       final int cancelThreshold) throws InterruptedException {

        final List<String> ids = new ArrayList<>(messages.size());
        for (int i = 0; i < messages.size(); i++) {
            ids.add(idPrefix + i);
        }

        return collectPackages(host, port, headers, ids, from, messages, interMessageSleepMs, lockStepMode, cancelThreshold);
    }

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(final String... args) throws Exception {
        /* Access a service running on the local machine on port 8080 */
        final long startTimeMillis = System.currentTimeMillis();
        final String host = "localhost";
        final int port = 8080;
        final HashMap<String, Object> headers = new HashMap<>();

        if (false) {
            final String id = UUID.randomUUID().toString() + ".SEND_ERROR";
            final String user = "Jim";
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100_000; i++) {
                sb.append("a");
                if (i % 1000 == 0) {
                    sb.append(".");
                }
            }

            headers.put("x-cid", "cid-send-package." + startTimeMillis);
            final CourierReply courierReply = sendPackage(host, port, headers, id, user, sb.toString());
            logFunction.apply("sendPackage response = " + courierReply);
        }

        if (false) {
            headers.put("x-cid", "cid-collect-packages-complete." + startTimeMillis);
            final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
            final List<CourierSummary> courierSummaries =
                collectPackages(host, port, headers, "id-", "User", messages, 100, true, messages.size());
            logFunction.apply("collectPackages response size = " + courierSummaries.size());
            if (!courierSummaries.isEmpty()) {
                final CourierSummary courierSummary = courierSummaries.get(courierSummaries.size() - 1);
                logFunction.apply("collectPackages[complete] summary = " + courierSummary.toString());
            }
        }

        if (false) {
            headers.put("x-cid", "cid-collect-packages-cancel." + startTimeMillis);
            final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
            final List<CourierSummary> courierSummaries =
                collectPackages(host, port, headers, "id-", "User", messages, 100, true, messages.size() / 2);
            logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        }

        if (false) {
            headers.put("x-cid", "cid-collect-packages-server-pre-cancel." + startTimeMillis);
            final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
            ids.set(5, ids.get(5) + ".EXIT_PRE_RESPONSE");
            final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
            final List<CourierSummary> courierSummaries =
                collectPackages(host, port, headers, ids, "User", messages, 100, true, messages.size() + 1);
            logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        }

        if (false) {
            headers.put("x-cid", "cid-collect-packages-server-post-cancel." + startTimeMillis);
            final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
            ids.set(5, ids.get(5) + ".EXIT_POST_RESPONSE");
            final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
            final List<CourierSummary> courierSummaries =
                collectPackages(host, port, headers, ids, "User", messages, 100, true, messages.size() + 1);
            logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        }

        if (true) {
            headers.put("x-cid", "cid-collect-packages-server-error." + startTimeMillis);
            final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
            ids.set(5, ids.get(5) + ".SEND_ERROR");
            final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
            final List<CourierSummary> courierSummaries =
                collectPackages(host, port, headers, ids, "User", messages, 100, true, messages.size() + 1);
            logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        }
    }
}
