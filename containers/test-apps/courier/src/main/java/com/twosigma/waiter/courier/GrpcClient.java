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

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GrpcClient {

    private final static Logger LOGGER = Logger.getLogger(GrpcServer.class.getName());

    public static final class RpcResult<Result> {
        private final Result result;
        private final Status status;

        private RpcResult(final Result result, final Status status) {
            this.result = result;
            this.status = status;
        }

        public Result result() {
            return result;
        }

        public Status status() {
            return status;
        }
    }

    public enum CancellationPolicy {

        CONTEXT() {
            @Override
            public void apply(
                final Context.CancellableContext cancellableContext,
                final ClientCallStreamObserver<?> observer,
                final String message) {
                cancellableContext.cancel(new RuntimeException(message));
            }
        },
        EXCEPTION() {
            @Override
            public void apply(final Context.CancellableContext cancellableContext,
                              final ClientCallStreamObserver<?> observer,
                              final String message) {
                throw new CancellationException(message);
            }
        },
        NONE() {
            @Override
            public void apply(final Context.CancellableContext cancellableContext,
                              final ClientCallStreamObserver<?> observer,
                              final String message) {
                // do nothing
            }
        },
        OBSERVER() {
            @Override
            public void apply(final Context.CancellableContext cancellableContext,
                              final ClientCallStreamObserver<?> observer,
                              final String message) {
                if (observer == null) {
                    throw new IllegalStateException("Cannot cancel when stream observer is missing!");
                } else {
                    final StatusRuntimeException error = Status.CANCELLED
                        .withDescription("Observer: " + message)
                        .asRuntimeException();
                    observer.cancel(message, error);
                }
            }
        };

        public abstract void apply(final Context.CancellableContext cancellableContext,
                                   final ClientCallStreamObserver<?> observer,
                                   final String message);
    }

    private static Variant retrieveVariant(final String id) {
        if (id.contains("SEND_ERROR")) {
            return Variant.SEND_ERROR;
        } else if (id.contains("EXIT_PRE_RESPONSE")) {
            return Variant.EXIT_PRE_RESPONSE;
        } else if (id.contains("EXIT_POST_RESPONSE")) {
            return Variant.EXIT_POST_RESPONSE;
        } else {
            return Variant.NORMAL;
        }
    }

    private final Function<String, Void> logFunction;
    private final String host;
    private final int port;

    public GrpcClient(final String host, final int port) {
        this(host, port, new Function<String, Void>() {
            @Override
            public Void apply(final String message) {
                LOGGER.info(message);
                return null;
            }
        });
    }

    public GrpcClient(final String host, final int port, final Function<String, Void> logFunction) {
        this.host = host;
        this.port = port;
        this.logFunction = logFunction;
    }

    private ManagedChannel initializeChannel() {
        logFunction.apply("initializing plaintext client at " + host + ":" + port);
        return ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext()
            .build();
    }

    private void shutdownChannel(final ManagedChannel channel) {
        logFunction.apply("shutting down channel");
        try {
            channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
            if (channel.isShutdown()) {
                logFunction.apply("channel shutdown successfully");
            } else {
                logFunction.apply("channel shutdown timed out!");
            }
        } catch (Exception ex) {
            logFunction.apply("error in channel shutdown: " + ex.getMessage());
        }
    }

    private Metadata createRequestHeadersMetadata(final Map<String, Object> headers) {
        final Metadata headerMetadata = new Metadata();
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            final String key = entry.getKey();
            final String value = String.valueOf(entry.getValue());
            headerMetadata.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);
        }
        return headerMetadata;
    }

    private Channel wrapResponseLogger(final ManagedChannel channel) {
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

    public RpcResult<StateReply> retrieveState(final Map<String, Object> headers,
                                               final String correlationId) {
        final ManagedChannel channel = initializeChannel();

        try {
            final Channel wrappedChannel = wrapResponseLogger(channel);
            final Metadata headerMetadata = createRequestHeadersMetadata(headers);

            final CourierGrpc.CourierFutureStub rawStub = CourierGrpc.newFutureStub(wrappedChannel);
            final CourierGrpc.CourierFutureStub futureStub = MetadataUtils.attachHeaders(rawStub, headerMetadata);

            logFunction.apply("will try to retrieve state for " + correlationId + " ...");
            final StateRequest request = StateRequest
                .newBuilder()
                .setCid(correlationId)
                .build();

            final AtomicReference<Status> status = new AtomicReference<>();
            final AtomicReference<StateReply> response = new AtomicReference<>();
            try {
                final StateReply reply = futureStub.retrieveState(request).get();
                status.set(Status.OK);
                response.set(reply);
            } catch (final StatusRuntimeException ex) {
                final Status errorStatus = ex.getStatus();
                logFunction.apply("RPC failed, status: " + errorStatus);
                status.set(errorStatus);
            } catch (final ExecutionException ex) {
                final Status errorStatus = Status.fromThrowable(ex.getCause());
                logFunction.apply("RPC execution failed: " + errorStatus);
                status.set(errorStatus);
            } catch (final Throwable th) {
                logFunction.apply("RPC failed, message: " + th.getMessage());
                status.set(Status.UNKNOWN.withDescription(th.getMessage()));
            }

            if (response.get() != null) {
                final StateReply reply = response.get();
                logFunction.apply("received response StateReply{" +
                    "cid=" + reply.getCid() + ", " +
                    "response=" + reply.getStateList() + "}");
            }
            return new RpcResult<>(response.get(), status.get());

        } finally {
            shutdownChannel(channel);
        }
    }

    public RpcResult<CourierReply> sendPackage(final Map<String, Object> headers,
                                               final String id,
                                               final String from,
                                               final String message,
                                               final long sleepDurationMillis,
                                               final long deadlineDurationMillis) {
        final ManagedChannel channel = initializeChannel();

        try {
            final Channel wrappedChannel = wrapResponseLogger(channel);
            final Metadata headerMetadata = createRequestHeadersMetadata(headers);

            final CourierGrpc.CourierFutureStub rawStub =
                CourierGrpc.newFutureStub(wrappedChannel).withDeadlineAfter(deadlineDurationMillis, TimeUnit.MILLISECONDS);
            final CourierGrpc.CourierFutureStub futureStub = MetadataUtils.attachHeaders(rawStub, headerMetadata);

            logFunction.apply("will try to send package from " + from + " ...");
            final CourierRequest request = CourierRequest
                .newBuilder()
                .setId(id)
                .setFrom(from)
                .setMessage(message)
                .setVariant(retrieveVariant(id))
                .setSleepDurationMillis(sleepDurationMillis)
                .build();

            final AtomicReference<Status> status = new AtomicReference<>();
            final AtomicReference<CourierReply> response = new AtomicReference<>();
            try {
                final CourierReply reply = futureStub.sendPackage(request).get();
                status.set(Status.OK);
                response.set(reply);
            } catch (final StatusRuntimeException ex) {
                final Status errorStatus = ex.getStatus();
                logFunction.apply("RPC failed, status: " + errorStatus);
                status.set(errorStatus);
            } catch (final ExecutionException ex) {
                final Status errorStatus = Status.fromThrowable(ex.getCause());
                logFunction.apply("RPC execution failed: " + errorStatus);
                status.set(errorStatus);
            } catch (final Throwable th) {
                logFunction.apply("RPC failed, message: " + th.getMessage());
                status.set(Status.UNKNOWN.withDescription(th.getMessage()));
            }

            final CourierReply reply = response.get();
            if (reply != null) {
                logFunction.apply("received response CourierReply{" +
                    "id=" + reply.getId() + ", " +
                    "response=" + reply.getResponse() + ", " +
                    "message.length=" + reply.getMessage().length() + "}");
                logFunction.apply("messages equal = " + message.equals(reply.getMessage()));
            }
            return new RpcResult<>(reply, status.get());

        } finally {
            shutdownChannel(channel);
        }
    }

    public RpcResult<List<CourierSummary>> collectPackages(final Map<String, Object> headers,
                                                           final List<String> ids,
                                                           final String from,
                                                           final List<String> messages,
                                                           final int interMessageSleepMs,
                                                           final boolean lockStepMode,
                                                           final int cancelThreshold,
                                                           final CancellationPolicy cancellationPolicy,
                                                           final long deadlineDurationMillis) {
        final ManagedChannel channel = initializeChannel();
        final AtomicBoolean awaitChannelTermination = new AtomicBoolean(true);

        final Context.CancellableContext cancellableContext = Context.current().withCancellation();
        try {
            final Semaphore lockStep = new Semaphore(1);
            final AtomicBoolean errorSignal = new AtomicBoolean(false);

            final Channel wrappedChannel = wrapResponseLogger(channel);
            final Metadata headerMetadata = createRequestHeadersMetadata(headers);

            final CourierGrpc.CourierStub rawStub =
                CourierGrpc.newStub(wrappedChannel).withDeadlineAfter(deadlineDurationMillis, TimeUnit.MILLISECONDS);
            final CourierGrpc.CourierStub futureStub = MetadataUtils.attachHeaders(rawStub, headerMetadata);

            logFunction.apply("will try to send package from " + from + " ...");

            final AtomicReference<ClientCallStreamObserver> observer = new AtomicReference<>();
            final AtomicReference<Status> status = new AtomicReference<>();
            final AtomicReference<List<CourierSummary>> response = new AtomicReference<>();

            cancellableContext.run(() -> {
                final CompletableFuture<List<CourierSummary>> responsePromise = new CompletableFuture<>();
                try {
                    final List<CourierSummary> resultList = new ArrayList<>();

                    final StreamObserver<CourierRequest> collector =
                        futureStub.collectPackages(new ClientResponseObserver<CourierRequest, CourierSummary>() {

                            @Override
                            public void beforeStart(final ClientCallStreamObserver<CourierRequest> requestStream) {
                                observer.set(requestStream);
                            }

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
                            public void onError(final Throwable th) {
                                logFunction.apply("error in collecting summaries " + th);
                                errorSignal.compareAndSet(false, true);
                                if (lockStepMode) {
                                    logFunction.apply("releasing semaphore after receiving error");
                                    lockStep.release();
                                }
                                if (th instanceof StatusRuntimeException) {
                                    final StatusRuntimeException exception = (StatusRuntimeException) th;
                                    status.set(exception.getStatus());
                                } else {
                                    status.set(Status.UNKNOWN.withDescription(th.getMessage()));
                                }
                                resolveResponsePromise();
                            }

                            @Override
                            public void onCompleted() {
                                logFunction.apply("completed collecting summaries");
                                status.set(Status.OK);
                                resolveResponsePromise();
                            }

                            private void resolveResponsePromise() {
                                logFunction.apply("client result has " + resultList.size() + " entries");
                                responsePromise.complete(resultList);
                            }
                        });

                    try {
                        for (int i = 0; i < messages.size(); i++) {
                            if (i >= cancelThreshold) {
                                logFunction.apply("cancelling sending messages");
                                awaitChannelTermination.set(false);

                                final String cancellationMessage = "Cancel threshold reached: " + cancelThreshold + " -> " + cancellationPolicy;
                                cancellationPolicy.apply(cancellableContext, observer.get(), cancellationMessage);
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
                                .setVariant(retrieveVariant(requestId))
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
                    } catch (final Throwable th) {
                        logFunction.apply("client threw exception, result has " + resultList.size() + " entries");
                        Thread.sleep(interMessageSleepMs);
                        responsePromise.complete(resultList);
                        throw th;
                    } finally {
                        response.set(responsePromise.get());
                    }
                } catch (final StatusRuntimeException ex) {
                    logFunction.apply("RPC failed, status: " + ex.getStatus());
                    status.set(ex.getStatus());
                } catch (final Exception ex) {
                    logFunction.apply("RPC failed, message: " + ex.getMessage());
                    status.set(Status.UNKNOWN.withDescription(ex.getMessage()));
                }
            });

            return new RpcResult<>(response.get(), status.get());
        } finally {
            if (awaitChannelTermination.get()) {
                shutdownChannel(channel);
            } else {
                channel.shutdownNow();
            }
            cancellableContext.cancel(null);
        }
    }

    public RpcResult<CourierSummary> aggregatePackages(final Map<String, Object> headers,
                                                       final List<String> ids,
                                                       final String from,
                                                       final List<String> messages,
                                                       final int interMessageSleepMs,
                                                       final int cancelThreshold,
                                                       final CancellationPolicy cancellationPolicy,
                                                       final long deadlineDurationMillis) {
        final ManagedChannel channel = initializeChannel();
        final AtomicBoolean awaitChannelTermination = new AtomicBoolean(true);

        final Context.CancellableContext cancellableContext = Context.current().withCancellation();
        try {
            final AtomicBoolean errorSignal = new AtomicBoolean(false);

            final Channel wrappedChannel = wrapResponseLogger(channel);
            final Metadata headerMetadata = createRequestHeadersMetadata(headers);

            final CourierGrpc.CourierStub rawStub =
                CourierGrpc.newStub(wrappedChannel).withDeadlineAfter(deadlineDurationMillis, TimeUnit.MILLISECONDS);
            final CourierGrpc.CourierStub futureStub = MetadataUtils.attachHeaders(rawStub, headerMetadata);

            logFunction.apply("will try to aggregate package from " + from + " ...");

            final AtomicReference<ClientCallStreamObserver> observer = new AtomicReference<>();
            final AtomicReference<Status> status = new AtomicReference<>();
            final AtomicReference<CourierSummary> response = new AtomicReference<>();

            cancellableContext.run(() -> {
                final CompletableFuture<CourierSummary> responsePromise = new CompletableFuture<>();
                try {
                    final StreamObserver<CourierRequest> collector =
                        futureStub.aggregatePackages(new ClientResponseObserver<CourierRequest, CourierSummary>() {

                            @Override
                            public void beforeStart(final ClientCallStreamObserver<CourierRequest> requestStream) {
                                observer.set(requestStream);
                            }

                            @Override
                            public void onNext(final CourierSummary summary) {
                                logFunction.apply("received response CourierSummary{" +
                                    "count=" + summary.getNumMessages() + ", " +
                                    "length=" + summary.getTotalLength() + "}");
                                response.set(summary);
                            }

                            @Override
                            public void onError(final Throwable th) {
                                logFunction.apply("error in aggregating summaries " + th);
                                errorSignal.compareAndSet(false, true);
                                if (th instanceof StatusRuntimeException) {
                                    final StatusRuntimeException exception = (StatusRuntimeException) th;
                                    status.set(exception.getStatus());
                                } else {
                                    status.set(Status.UNKNOWN.withDescription(th.getMessage()));
                                }
                                resolveResponsePromise();
                            }

                            @Override
                            public void onCompleted() {
                                logFunction.apply("completed aggregating summaries");
                                status.set(Status.OK);
                                resolveResponsePromise();
                            }

                            private void resolveResponsePromise() {
                                final CourierSummary courierSummary = response.get();
                                logFunction.apply("client result: " + courierSummary);
                                responsePromise.complete(courierSummary);
                            }
                        });

                    for (int i = 0; i < messages.size(); i++) {
                        if (i >= cancelThreshold) {
                            logFunction.apply("cancelling sending messages");
                            awaitChannelTermination.set(false);
                            final String cancellationMessage = "Cancel threshold reached: " + cancelThreshold + " -> " + cancellationPolicy;
                            cancellationPolicy.apply(cancellableContext, observer.get(), cancellationMessage);
                        }
                        if (errorSignal.get()) {
                            logFunction.apply("aborting sending messages as error was discovered");
                            break;
                        }
                        final String requestId = ids.get(i);
                        final CourierRequest request = CourierRequest
                            .newBuilder()
                            .setId(requestId)
                            .setFrom(from)
                            .setMessage(messages.get(i))
                            .setVariant(retrieveVariant(requestId))
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

                    responsePromise.get();
                } catch (final StatusRuntimeException ex) {
                    logFunction.apply("RPC failed, status: " + ex.getStatus());
                    status.set(ex.getStatus());
                } catch (final Exception ex) {
                    logFunction.apply("RPC failed, message: " + ex.getMessage());
                    status.set(Status.UNKNOWN.withDescription(ex.getMessage()));
                }
            });

            return new RpcResult<>(response.get(), status.get());

        } finally {
            if (awaitChannelTermination.get()) {
                shutdownChannel(channel);
            } else {
                channel.shutdownNow();
            }
            logFunction.apply("close the context and trigger notification of listeners");
            cancellableContext.cancel(null);
        }
    }

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(final String... args) throws Exception {
        /* Access a service running on the local machine on port 8080 */
        final String host = args.length > 0 ? args[0] : "localhost";
        final int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        final String methodName = args.length > 2 ? args[2] : "runCollectPackagesClientCancelObserver";
        final String correlationId = args.length > 3 ? args[3] : ("courier-request-" + System.nanoTime());

        System.out.println("host = " + host);
        System.out.println("port = " + port);
        System.out.println("correlationId = " + correlationId);
        System.out.println("methodName = " + methodName);

        LoggingConfig.initializeLogging();

        final GrpcClient client = new GrpcClient(host, port);

        // avoid reflection, and explicitly reference method names to keep unused tracker happy
        switch (methodName) {
            case "runAggregatePackagesClientContext":
                runAggregatePackagesClientContext(client, correlationId);
                break;
            case "runAggregatePackagesClientCancelException":
                runAggregatePackagesClientCancelException(client, correlationId);
                break;
            case "runAggregatePackagesClientCancelObserver":
                runAggregatePackagesClientCancelObserver(client, correlationId);
                break;
            case "runAggregatePackagesExitPreResponse":
                runAggregatePackagesExitPreResponse(client, correlationId);
                break;
            case "runAggregatePackagesSendError":
                runAggregatePackagesSendError(client, correlationId);
                break;
            case "runAggregatePackagesSuccess":
                runAggregatePackagesSuccess(client, correlationId);
                break;
            case "runCollectPackagesClientCancelContext":
                runCollectPackagesClientCancelContext(client, correlationId);
                break;
            case "runCollectPackagesClientCancelException":
                runCollectPackagesClientCancelException(client, correlationId);
                break;
            case "runCollectPackagesClientCancelObserver":
                runCollectPackagesClientCancelObserver(client, correlationId);
                break;
            case "runCollectPackagesExitPostResponse":
                runCollectPackagesExitPostResponse(client, correlationId);
                break;
            case "runCollectPackagesExitPreResponse":
                runCollectPackagesExitPreResponse(client, correlationId);
                break;
            case "runCollectPackagesSendError":
                runCollectPackagesSendError(client, correlationId);
                break;
            case "runCollectPackagesSuccess":
                runCollectPackagesSuccess(client, correlationId);
                break;
            case "runSendPackageSendError":
                runSendPackageSendError(client, correlationId);
                break;
            case "runSendPackageSuccess":
                runSendPackageSuccess(client, correlationId);
                break;
            default:
                throw new IllegalArgumentException("Unsupported method: " + methodName);
        }
    }

    private static void retrieveStateForCid(final GrpcClient client,
                                            final Map<String, Object> headers,
                                            final String correlationId) {
        final String retrieveCid = "cid-retrieve-state." + System.currentTimeMillis();
        headers.put("x-cid", retrieveCid);
        final RpcResult<StateReply> rpcStateResult = client.retrieveState(headers, correlationId);
        final StateReply stateReply = rpcStateResult.result();
        client.logFunction.apply("retrieveState response = " + stateReply);
        final Status retrieveStatus = rpcStateResult.status();
        client.logFunction.apply("retrieveState status = " + retrieveStatus);
    }

    private static void runSendPackageSuccess(final GrpcClient client, final String correlationId) {
        final String id = UUID.randomUUID().toString();
        final String user = "Jim";
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100_000; i++) {
            sb.append("a");
            if (i % 1000 == 0) {
                sb.append(".");
            }
        }

        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final RpcResult<CourierReply> rpcResult = client.sendPackage(headers, id, user, sb.toString(), 10, 1000);
        final CourierReply courierReply = rpcResult.result();
        client.logFunction.apply("sendPackage response = " + courierReply);
        final Status status = rpcResult.status();
        client.logFunction.apply("sendPackage status = " + status);
        retrieveStateForCid(client, headers, correlationId);
    }

    private static void runSendPackageSendError(final GrpcClient client, final String correlationId) {
        final String id = UUID.randomUUID().toString() + ".SEND_ERROR";
        final String user = "Jim";
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100_000; i++) {
            sb.append("a");
            if (i % 1000 == 0) {
                sb.append(".");
            }
        }

        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final RpcResult<CourierReply> rpcResult = client.sendPackage(headers, id, user, sb.toString(), 10, 1000);
        final CourierReply courierReply = rpcResult.result();
        client.logFunction.apply("sendPackage response = " + courierReply);
        final Status status = rpcResult.status();
        client.logFunction.apply("sendPackage status = " + status);
        retrieveStateForCid(client, headers, correlationId);
    }

    private static void runCollectPackagesSuccess(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<List<CourierSummary>> rpcResult =
            client.collectPackages(headers, ids, "User", messages, 100, true,
                messages.size() + 1, CancellationPolicy.NONE, 10000);
        final List<CourierSummary> courierSummaries = rpcResult.result();
        client.logFunction.apply("collectPackages[success] summary = " + courierSummaries);
        final Status status = rpcResult.status();
        client.logFunction.apply("collectPackages[success] status = " + status);
    }

    private static void runCollectPackagesClientCancelContext(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        ids.set(5, ids.get(5) + ".SEND_ERROR");
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<List<CourierSummary>> rpcResult =
            client.collectPackages(headers, ids, "User", messages, 100, true,
                messages.size() / 2, CancellationPolicy.CONTEXT, 10000);
        final List<CourierSummary> courierSummaries = rpcResult.result();
        client.logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        final Status status = rpcResult.status();
        client.logFunction.apply("collectPackages[cancel] status = " + status);
        retrieveStateForCid(client, headers, correlationId);
    }

    private static void runCollectPackagesClientCancelException(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        ids.set(5, ids.get(5) + ".SEND_ERROR");
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<List<CourierSummary>> rpcResult =
            client.collectPackages(headers, ids, "User", messages, 100, true,
                messages.size() / 2, CancellationPolicy.EXCEPTION, 10000);
        final List<CourierSummary> courierSummaries = rpcResult.result();
        client.logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        final Status status = rpcResult.status();
        client.logFunction.apply("collectPackages[cancel] status = " + status);
        retrieveStateForCid(client, headers, correlationId);
    }

    private static void runCollectPackagesClientCancelObserver(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        ids.set(5, ids.get(5) + ".SEND_ERROR");
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<List<CourierSummary>> rpcResult =
            client.collectPackages(headers, ids, "User", messages, 100, true,
                messages.size() / 2, CancellationPolicy.OBSERVER, 10000);
        final List<CourierSummary> courierSummaries = rpcResult.result();
        client.logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        final Status status = rpcResult.status();
        client.logFunction.apply("collectPackages[cancel] status = " + status);
        retrieveStateForCid(client, headers, correlationId);
    }

    private static void runCollectPackagesSendError(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        ids.set(5, ids.get(5) + ".SEND_ERROR");
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<List<CourierSummary>> rpcResult =
            client.collectPackages(headers, ids, "User", messages, 100, true,
                messages.size() + 1, CancellationPolicy.EXCEPTION, 10000);
        final List<CourierSummary> courierSummaries = rpcResult.result();
        client.logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        final Status status = rpcResult.status();
        client.logFunction.apply("collectPackages[cancel] status = " + status);
    }

    private static void runCollectPackagesExitPreResponse(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        ids.set(5, ids.get(5) + ".EXIT_PRE_RESPONSE");
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<List<CourierSummary>> rpcResult =
            client.collectPackages(headers, ids, "User", messages, 100, true,
                messages.size() + 1, CancellationPolicy.EXCEPTION, 10000);
        final List<CourierSummary> courierSummaries = rpcResult.result();
        client.logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        final Status status = rpcResult.status();
        client.logFunction.apply("collectPackages[cancel] status = " + status);
    }

    private static void runCollectPackagesExitPostResponse(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        ids.set(5, ids.get(5) + ".EXIT_POST_RESPONSE");
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<List<CourierSummary>> rpcResult =
            client.collectPackages(headers, ids, "User", messages, 100, true,
                messages.size() + 1, CancellationPolicy.EXCEPTION, 10000);
        final List<CourierSummary> courierSummaries = rpcResult.result();
        client.logFunction.apply("collectPackages[cancel] summary = " + courierSummaries);
        final Status status = rpcResult.status();
        client.logFunction.apply("collectPackages[cancel] status = " + status);
    }

    private static void runAggregatePackagesSuccess(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<CourierSummary> rpcResult =
            client.aggregatePackages(headers, ids, "User", messages, 100,
                messages.size() + 1, CancellationPolicy.EXCEPTION, 10000);
        final CourierSummary courierSummary = rpcResult.result();
        client.logFunction.apply("aggregatePackages[success] summary = " + courierSummary);
        final Status status = rpcResult.status();
        client.logFunction.apply("aggregatePackages[success] status = " + status);
    }

    private static void runAggregatePackagesClientCancel(final GrpcClient client) {
        final HashMap<String, Object> headers = new HashMap<>();
        final String correlationId = "cid-aggregate-packages-client-cancel." + System.currentTimeMillis();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<CourierSummary> rpcResult =
            client.aggregatePackages(headers, ids, "User", messages, 100,
                messages.size() / 2, CancellationPolicy.EXCEPTION);
        final CourierSummary courierSummary = rpcResult.result();
        client.logFunction.apply("aggregatePackages[success] summary = " + courierSummary);
        final Status status = rpcResult.status();
        client.logFunction.apply("aggregatePackages[success] status = " + status);
        retrieveStateForCid(client, headers, correlationId);
    }

    private static void runAggregatePackagesClientContext(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<CourierSummary> rpcResult =
            client.aggregatePackages(headers, ids, "User", messages, 100,
                messages.size() / 2, CancellationPolicy.CONTEXT, 10000);
        final CourierSummary courierSummary = rpcResult.result();
        client.logFunction.apply("aggregatePackages[success] summary = " + courierSummary);
        final Status status = rpcResult.status();
        client.logFunction.apply("aggregatePackages[success] status = " + status);
        retrieveStateForCid(client, headers, correlationId);
    }

    private static void runAggregatePackagesClientCancelException(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<CourierSummary> rpcResult =
            client.aggregatePackages(headers, ids, "User", messages, 100,
                messages.size() / 2, CancellationPolicy.EXCEPTION, 10000);
        final CourierSummary courierSummary = rpcResult.result();
        client.logFunction.apply("aggregatePackages[success] summary = " + courierSummary);
        final Status status = rpcResult.status();
        client.logFunction.apply("aggregatePackages[success] status = " + status);
        retrieveStateForCid(client, headers, correlationId);
    }

    private static void runAggregatePackagesClientCancelObserver(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<CourierSummary> rpcResult =
            client.aggregatePackages(headers, ids, "User", messages, 100,
                messages.size() / 2, CancellationPolicy.OBSERVER, 10000);
        final CourierSummary courierSummary = rpcResult.result();
        client.logFunction.apply("aggregatePackages[success] summary = " + courierSummary);
        final Status status = rpcResult.status();
        client.logFunction.apply("aggregatePackages[success] status = " + status);
        retrieveStateForCid(client, headers, correlationId);
    }

    private static void runAggregatePackagesSendError(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        ids.set(5, ids.get(5) + ".SEND_ERROR");
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<CourierSummary> rpcResult =
            client.aggregatePackages(headers, ids, "User", messages, 100,
                messages.size() + 1, CancellationPolicy.EXCEPTION, 10000);
        final CourierSummary courierSummary = rpcResult.result();
        client.logFunction.apply("aggregatePackages[cancel] summary = " + courierSummary);
        final Status status = rpcResult.status();
        client.logFunction.apply("aggregatePackages[cancel] status = " + status);
    }

    private static void runAggregatePackagesExitPreResponse(final GrpcClient client, final String correlationId) {
        final HashMap<String, Object> headers = new HashMap<>();
        headers.put("x-cid", correlationId);
        final List<String> ids = IntStream.range(0, 10).mapToObj(i -> "id-" + i).collect(Collectors.toList());
        ids.set(5, ids.get(5) + ".EXIT_PRE_RESPONSE");
        final List<String> messages = IntStream.range(0, 10).mapToObj(i -> "message-" + i).collect(Collectors.toList());
        final RpcResult<CourierSummary> rpcResult =
            client.aggregatePackages(headers, ids, "User", messages, 100,
                messages.size() + 1, CancellationPolicy.EXCEPTION, 10000);
        final CourierSummary courierSummary = rpcResult.result();
        client.logFunction.apply("aggregatePackages[cancel] summary = " + courierSummary);
        final Status status = rpcResult.status();
        client.logFunction.apply("aggregatePackages[cancel] status = " + status);
    }
}
