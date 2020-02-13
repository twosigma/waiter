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

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class GrpcServer {

    private final static Logger LOGGER = Logger.getLogger(GrpcServer.class.getName());
    private final static Context.Key<CidTimestamp> CID_TIMESTAMP = Context.key("CID.TIMESTAMP");
    private final static Map<String, Map<Long, List<String>>> requestCidToStateList = new HashMap<>();

    private static List<String> trackState(final String correlationId, final long timestamp, final String state) {
        if (correlationId != null) {
            synchronized (requestCidToStateList) {
                final List<String> stateList;

                final Map<Long, List<String>> stateEntriesCurrent = requestCidToStateList.get(correlationId);
                if (stateEntriesCurrent != null) {
                    final List<String> stateListCurrent = stateEntriesCurrent.get(timestamp);
                    if (stateListCurrent == null) {
                        stateList = new ArrayList<>();
                        stateEntriesCurrent.put(timestamp, stateList);
                    } else {
                        stateList = stateListCurrent;
                    }
                } else {
                    stateList = new ArrayList<>();
                    final Map<Long, List<String>> stateEntriesNew = new HashMap<>();
                    stateEntriesNew.put(timestamp, stateList);
                    requestCidToStateList.put(correlationId, stateEntriesNew);
                }

                stateList.add(state);
                return stateList;
            }
        }
        return new ArrayList<>();
    }

    private static List<String> trackState(final String correlationId) {
        synchronized (requestCidToStateList) {
            final Map<Long, List<String>> stateEntries = requestCidToStateList.get(correlationId);
            if (stateEntries != null && !stateEntries.isEmpty()) {
                final List<Long> timestamps = new ArrayList<Long>(stateEntries.keySet());
                Collections.sort(timestamps);
                final Long requestTime = timestamps.get(0);
                final List<String> stateList = stateEntries.get(requestTime);
                if (stateList == null) {
                    return null;
                } else {
                    return new ArrayList<>(stateList);
                }
            } else {
                return new ArrayList<>();
            }
        }
    }

    private static void sleep(final long durationMillis) {
        if (durationMillis > 0) {
            try {
                Thread.sleep(durationMillis);
            } catch (final Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void registerOnCancelHandler(final StreamObserver<?> responseObserver) {
        if (responseObserver instanceof ServerCallStreamObserver) {
            final CidTimestamp cidTimestamp = CID_TIMESTAMP.get();
            final String correlationId = cidTimestamp.correlationId;
            final long timestamp = cidTimestamp.timestamp;

            ((ServerCallStreamObserver) responseObserver).setOnCancelHandler(() -> {
                LOGGER.info("CancelHandler for " + cidTimestamp + " was triggered");
                trackState(correlationId, timestamp, "CANCEL_HANDLER");
            });
        }
    }

    private Server server;

    void start(final int port) throws IOException {
        LOGGER.info("starting gRPC server on port " + port);
        server = ServerBuilder
            .forPort(port)
            .addService(new CourierImpl())
            .intercept(new GrpcServerInterceptor())
            .intercept(new CorrelationIdInterceptor())
            .build()
            .start();
        LOGGER.info("gRPC server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            GrpcServer.this.stop();
            System.err.println("*** server shut down");
        }));
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static class CidTimestamp {
        private final String correlationId;
        private final long timestamp;

        private CidTimestamp(String correlationId, long timestamp) {
            this.correlationId = correlationId;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "CidTimestamp{" +
                "correlationId='" + correlationId + '\'' +
                ", timestamp=" + timestamp +
                '}';
        }
    }

    private static class CourierImpl extends CourierGrpc.CourierImplBase {

        @Override
        public void retrieveState(final StateRequest request, final StreamObserver<StateReply> responseObserver) {
            final String correlationId = request.getCid();
            LOGGER.info("received StateRequest{" + "cid=" + correlationId + "}");

            registerOnCancelHandler(responseObserver);
            final StateReply.Builder builder = StateReply
                .newBuilder()
                .setCid(correlationId);
            final List<String> stateList = trackState(correlationId);
            LOGGER.info("cid " + correlationId + " has states: " + stateList);
            if (stateList != null) {
                for (final String state : stateList) {
                    builder.addState(state);
                }
            }
            final StateReply reply = builder.build();
            LOGGER.info("Sending StateReply for cid=" + reply.getCid());
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void sendPackage(final CourierRequest request, final StreamObserver<CourierReply> responseObserver) {
            LOGGER.info("received CourierRequest{" +
                "id=" + request.getId() + ", " +
                "from=" + request.getFrom() + ", " +
                "message.length=" + request.getMessage().length() + "}");
            registerOnCancelHandler(responseObserver);
            sleep(request.getSleepDurationMillis());
            if (Variant.SEND_ERROR.equals(request.getVariant())) {
                final StatusRuntimeException error = Status.CANCELLED
                    .withCause(new RuntimeException(request.getId()))
                    .withDescription("Cancelled by server")
                    .asRuntimeException();
                LOGGER.info("Sending cancelled by server error");
                responseObserver.onError(error);
            } else if (Variant.EXIT_PRE_RESPONSE.equals(request.getVariant())) {
                sleep(1000);
                LOGGER.info("Exiting server abruptly");
                System.exit(1);
            } else {
                final CourierReply reply = CourierReply
                    .newBuilder()
                    .setId(request.getId())
                    .setMessage(request.getMessage())
                    .setResponse("received")
                    .build();
                LOGGER.info("Sending CourierReply for id=" + reply.getId());
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        }

        @Override
        public StreamObserver<CourierRequest> collectPackages(final StreamObserver<CourierSummary> responseObserver) {

            registerOnCancelHandler(responseObserver);
            return new StreamObserver<CourierRequest>() {

                private long numMessages = 0;
                private long totalLength = 0;

                @Override
                public void onNext(final CourierRequest request) {
                    LOGGER.info("Received CourierRequest id=" + request.getId());

                    numMessages += 1;
                    totalLength += request.getMessage().length();
                    LOGGER.info("Summary of collected packages: numMessages=" + numMessages +
                        " with totalLength=" + totalLength);

                    sleep(request.getSleepDurationMillis());

                    if (Variant.EXIT_PRE_RESPONSE.equals(request.getVariant())) {
                        sleep(1000);
                        LOGGER.info("Exiting server abruptly");
                        System.exit(1);
                    } else if (Variant.SEND_ERROR.equals(request.getVariant())) {
                        final StatusRuntimeException error = Status.CANCELLED
                            .withCause(new RuntimeException(request.getId()))
                            .withDescription("Cancelled by server")
                            .asRuntimeException();
                        LOGGER.info("Sending cancelled by server error");
                        responseObserver.onError(error);
                    } else {
                        final CourierSummary courierSummary = CourierSummary
                            .newBuilder()
                            .setNumMessages(numMessages)
                            .setTotalLength(totalLength)
                            .build();
                        LOGGER.info("Sending CourierSummary for id=" + request.getId());
                        responseObserver.onNext(courierSummary);
                    }

                    if (Variant.EXIT_POST_RESPONSE.equals(request.getVariant())) {
                        sleep(1000);
                        LOGGER.info("Exiting server abruptly");
                        System.exit(1);
                    }
                }

                @Override
                public void onError(final Throwable th) {
                    LOGGER.severe("Error in collecting packages: " + th.getMessage());
                    responseObserver.onError(th);
                }

                @Override
                public void onCompleted() {
                    LOGGER.severe("Completed collecting packages");
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public StreamObserver<CourierRequest> aggregatePackages(final StreamObserver<CourierSummary> responseObserver) {

            registerOnCancelHandler(responseObserver);
            return new StreamObserver<CourierRequest>() {

                private long numMessages = 0;
                private long totalLength = 0;

                @Override
                public void onNext(final CourierRequest request) {
                    LOGGER.info("Received CourierRequest id=" + request.getId());

                    numMessages += 1;
                    totalLength += request.getMessage().length();
                    LOGGER.info("Summary of collected packages: numMessages=" + numMessages +
                        " with totalLength=" + totalLength);

                    sleep(request.getSleepDurationMillis());

                    if (Variant.EXIT_PRE_RESPONSE.equals(request.getVariant()) || Variant.EXIT_POST_RESPONSE.equals(request.getVariant())) {
                        sleep(1000);
                        LOGGER.info("Exiting server abruptly");
                        System.exit(1);
                    } else if (Variant.SEND_ERROR.equals(request.getVariant())) {
                        final StatusRuntimeException error = Status.CANCELLED
                            .withCause(new RuntimeException(request.getId()))
                            .withDescription("Cancelled by server")
                            .asRuntimeException();
                        LOGGER.info("Sending cancelled by server error");
                        responseObserver.onError(error);
                    }
                }

                @Override
                public void onError(final Throwable th) {
                    LOGGER.severe("Error in aggregating packages: " + th.getMessage());
                    responseObserver.onError(th);
                }

                @Override
                public void onCompleted() {
                    LOGGER.severe("Completed aggregating packages");
                    final CourierSummary courierSummary = CourierSummary
                        .newBuilder()
                        .setNumMessages(numMessages)
                        .setTotalLength(totalLength)
                        .build();
                    LOGGER.info("Sending aggregated CourierSummary");
                    responseObserver.onNext(courierSummary);
                    responseObserver.onCompleted();
                }
            };
        }
    }

    public class CorrelationIdInterceptor implements ServerInterceptor {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            final ServerCall<ReqT, RespT> serverCall,
            final Metadata requestMetadata,
            final ServerCallHandler<ReqT, RespT> serverCallHandler) {

            final Metadata.Key<String> xCidKey = Metadata.Key.of("x-cid", ASCII_STRING_MARSHALLER);
            String correlationId = requestMetadata.get(xCidKey);
            if (correlationId == null) {
                correlationId = "courier-" + System.nanoTime();
            }
            final long timestamp = System.nanoTime();

            final Context currentContext = Context.current();
            final Context newContext = currentContext.withValue(CID_TIMESTAMP, new CidTimestamp(correlationId, timestamp));

            return Contexts.interceptCall(newContext, serverCall, requestMetadata, serverCallHandler);
        }
    }

    private static class GrpcServerInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            final ServerCall<ReqT, RespT> serverCall,
            final Metadata requestMetadata,
            final ServerCallHandler<ReqT, RespT> serverCallHandler) {

            logMetadata(requestMetadata, "request");

            final CidTimestamp cidTimestamp = CID_TIMESTAMP.get();
            final String correlationId = cidTimestamp.correlationId;
            final long timestamp = cidTimestamp.timestamp;

            // handle duplicates with the same CID
            trackState(correlationId, timestamp, "INIT");

            final ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> wrapperCall =
                new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(serverCall) {
                    @Override
                    public void sendHeaders(final Metadata responseHeaders) {
                        LOGGER.info("GrpcServerInterceptor.sendHeaders[cid=" + correlationId + "]");
                        logMetadata(requestMetadata, "response");
                        if (correlationId != null) {
                            LOGGER.info("response linked to cid: " + correlationId);
                            responseHeaders.put(Metadata.Key.of("x-cid", ASCII_STRING_MARSHALLER), correlationId);
                            trackState(correlationId, timestamp, "SEND_HEADERS");
                        }
                        super.sendHeaders(responseHeaders);
                    }

                    @Override
                    public void sendMessage(final RespT response) {
                        LOGGER.info("GrpcServerInterceptor.sendMessage[cid=" + correlationId + "]");
                        trackState(correlationId, timestamp, "SEND_MESSAGE");
                        super.sendMessage(response);
                    }

                    @Override
                    public void close(final Status status, final Metadata trailers) {
                        LOGGER.info("GrpcServerInterceptor.close[cid=" + correlationId + "] " + status + ", " + trailers);
                        trackState(correlationId, timestamp, "CLOSE");
                        super.close(status, trailers);
                    }
                };
            final ServerCall.Listener<ReqT> listener = serverCallHandler.startCall(wrapperCall, requestMetadata);
            return new ServerCall.Listener<ReqT>() {
                public void onMessage(final ReqT message) {
                    LOGGER.info("GrpcServerInterceptor.onMessage[cid=" + correlationId + "]");
                    trackState(correlationId, timestamp, "RECEIVE_MESSAGE");
                    listener.onMessage(message);
                }

                public void onHalfClose() {
                    LOGGER.info("GrpcServerInterceptor.onHalfClose[cid=" + correlationId + "]");
                    trackState(correlationId, timestamp, "HALF_CLOSE");
                    listener.onHalfClose();
                }

                public void onCancel() {
                    LOGGER.info("GrpcServerInterceptor.onCancel[cid=" + correlationId + "]");
                    final List<String> stateList = trackState(correlationId, timestamp, "CANCEL");
                    LOGGER.info(correlationId + " states: " + stateList);
                    listener.onCancel();
                }

                public void onComplete() {
                    LOGGER.info("GrpcServerInterceptor.onComplete[cid=" + correlationId + "]");
                    final List<String> stateList = trackState(correlationId, timestamp, "COMPLETE");
                    LOGGER.info(correlationId + " states: " + stateList);
                    listener.onComplete();
                }

                public void onReady() {
                    LOGGER.info("GrpcServerInterceptor.onReady[cid=" + correlationId + "]");
                    trackState(correlationId, timestamp, "READY");
                    listener.onReady();
                }
            };
        }

        private void logMetadata(final Metadata metadata, final String label) {
            final Set<String> metadataKeys = metadata.keys();
            LOGGER.info(label + "@" + metadata.hashCode() + " metadata keys = " + metadataKeys);
            for (final String key : metadataKeys) {
                final String value = metadata.get(Metadata.Key.of(key, ASCII_STRING_MARSHALLER));
                LOGGER.info(label + " metadata " + key + " = " + value);
            }
        }
    }
}