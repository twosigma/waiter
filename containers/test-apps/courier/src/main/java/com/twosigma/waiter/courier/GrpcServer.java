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

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class GrpcServer {

    private final static Logger LOGGER = Logger.getLogger(GrpcServer.class.getName());
    private final static int MAX_STATES_TRACKED = 100;
    private final static Map<String, List<List<String>>> requestCidToStateList;

    static {
        requestCidToStateList = new LinkedHashMap<String, List<List<String>>>(MAX_STATES_TRACKED + 1, .75F, true) {
            // This method is called just after a new entry has been added
            public boolean removeEldestEntry(Map.Entry eldest) {
                return size() > MAX_STATES_TRACKED;
            }
        };
    }

    private static List<String> trackState(final String correlationId, final String state) {
        if (correlationId != null) {
            synchronized (requestCidToStateList) {
                final List<List<String>> statesList = requestCidToStateList.get(correlationId);
                if (statesList != null) {
                    final List<String> stateList = statesList.get(statesList.size() - 1);
                    stateList.add(state);
                    return stateList;
                } else {
                    LOGGER.info("Ignoring state " + state + " for " + correlationId);
                }
            }
        }
        return new ArrayList<>();
    }

    private static void trackState(final String correlationId, final List<String> stateList) {
        if (correlationId != null) {
            synchronized (requestCidToStateList) {
                if (!requestCidToStateList.containsKey(correlationId)) {
                    final ArrayList<List<String>> statesList = new ArrayList<>();
                    requestCidToStateList.put(correlationId, statesList);
                }
                final List<List<String>> statesList = requestCidToStateList.get(correlationId);
                statesList.add(stateList);
            }
        }
    }

    private static List<String> trackState(final String correlationId) {
        synchronized (requestCidToStateList) {
            final List<List<String>> statesList = requestCidToStateList.get(correlationId);
            if (statesList != null && !statesList.isEmpty()) {
                return statesList.get(0);
            } else {
                return new ArrayList<>();
            }
        }
    }


    private Server server;

    void start(final int port) throws IOException {
        LOGGER.info("starting gRPC server on port " + port);
        server = ServerBuilder
            .forPort(port)
            .addService(new CourierImpl())
            .intercept(new GrpcServerInterceptor())
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

    private static class CourierImpl extends CourierGrpc.CourierImplBase {

        @Override
        public void sendPackage(final CourierRequest request, final StreamObserver<CourierReply> responseObserver) {
            LOGGER.info("received CourierRequest{" +
                "id=" + request.getId() + ", " +
                "from=" + request.getFrom() + ", " +
                "message.length=" + request.getMessage().length() + "}");
            if (responseObserver instanceof ServerCallStreamObserver) {
                ((ServerCallStreamObserver) responseObserver).setOnCancelHandler(() -> {
                    LOGGER.info("CancelHandler:sendPackage CourierRequest{" + "id=" + request.getId() + "} was cancelled");
                });
            }
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

        @Override
        public void retrieveState(final StateRequest request, final StreamObserver<StateResponse> responseObserver) {
            final String correlationId = request.getCid();
            LOGGER.info("received StateRequest{" + "cid=" + correlationId + "}");

            if (responseObserver instanceof ServerCallStreamObserver) {
                ((ServerCallStreamObserver) responseObserver).setOnCancelHandler(() -> {
                    LOGGER.info("CancelHandler:retrieveState StateRequest{" + "cid=" + correlationId + "} was cancelled");
                });
            }
            final StateResponse.Builder builder = StateResponse
                .newBuilder()
                .setCid(correlationId);
            final List<String> stateList = trackState(correlationId);
            LOGGER.info("cid " + correlationId + " has states: " + stateList);
            if (stateList != null) {
                for (final String state : stateList) {
                    builder.addState(state);
                }
            }
            final StateResponse reply = builder.build();
            LOGGER.info("Sending StateResponse for cid=" + reply.getCid());
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<CourierRequest> collectPackages(final StreamObserver<CourierSummary> responseObserver) {

            if (responseObserver instanceof ServerCallStreamObserver) {
                ((ServerCallStreamObserver) responseObserver).setOnCancelHandler(() -> {
                    LOGGER.info("CancelHandler:collectPackages() was cancelled");
                });
            }
            return new StreamObserver<CourierRequest>() {

                private long numMessages = 0;
                private long totalLength = 0;

                @Override
                public void onNext(final CourierRequest courierRequest) {
                    LOGGER.info("Received CourierRequest id=" + courierRequest.getId());

                    numMessages += 1;
                    totalLength += courierRequest.getMessage().length();
                    LOGGER.severe("Summary of collected packages: numMessages=" + numMessages +
                        " with totalLength=" + totalLength);

                    if (courierRequest.getId().contains("EXIT_PRE_RESPONSE")) {
                        sleep(1000);
                        LOGGER.info("Exiting server abruptly");
                        System.exit(1);
                    }

                    final CourierSummary courierSummary = CourierSummary
                        .newBuilder()
                        .setNumMessages(numMessages)
                        .setTotalLength(totalLength)
                        .build();
                    LOGGER.info("Sending CourierSummary for id=" + courierRequest.getId());
                    responseObserver.onNext(courierSummary);

                    if (courierRequest.getId().contains("EXIT_POST_RESPONSE")) {
                        sleep(1000);
                        LOGGER.info("Exiting server abruptly");
                        System.exit(1);
                    }
                }

                private void sleep(final int durationMillis) {
                    try {
                        Thread.sleep(durationMillis);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onError(final Throwable throwable) {
                    LOGGER.severe("Error in collecting packages: " + throwable.getMessage());
                    responseObserver.onError(throwable);
                }

                @Override
                public void onCompleted() {
                    LOGGER.severe("Completed collecting packages");
                    responseObserver.onCompleted();
                }
            };
        }
    }

    private static class GrpcServerInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            final ServerCall<ReqT, RespT> serverCall,
            final Metadata requestMetadata,
            final ServerCallHandler<ReqT, RespT> serverCallHandler) {

            logMetadata(requestMetadata, "request");

            final Metadata.Key<String> xCidKey = Metadata.Key.of("x-cid", ASCII_STRING_MARSHALLER);
            final String correlationId = requestMetadata.get(xCidKey);

            // handle duplicates with the same CID
            final List<String> stateList = new ArrayList<>();
            stateList.add("INIT");
            trackState(correlationId, stateList);

            final ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> wrapperCall =
                new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(serverCall) {
                    @Override
                    public void sendHeaders(final Metadata responseHeaders) {
                        logMetadata(requestMetadata, "response");
                        if (correlationId != null) {
                            LOGGER.info("response linked to cid: " + correlationId);
                            responseHeaders.put(xCidKey, correlationId);
                            trackState(correlationId, "SEND_HEADERS");
                        }
                        super.sendHeaders(responseHeaders);
                    }

                    @Override
                    public void sendMessage(final RespT response) {
                        trackState(correlationId, "SEND_MESSAGE");
                        super.sendMessage(response);
                    }

                    @Override
                    public void close(final Status status, final Metadata trailers) {
                        LOGGER.info("GrpcServerInterceptor.close: " + status + ", " + trailers);
                        trackState(correlationId, "CLOSE");
                        super.close(status, trailers);
                    }
                };
            final ServerCall.Listener<ReqT> listener = serverCallHandler.startCall(wrapperCall, requestMetadata);
            return new ServerCall.Listener<ReqT>() {
                public void onMessage(final ReqT message) {
                    LOGGER.info("GrpcServerInterceptor.onMessage[cid=" + correlationId + "]");
                    trackState(correlationId, "RECEIVE_MESSAGE");
                    listener.onMessage(message);
                }

                public void onHalfClose() {
                    LOGGER.info("GrpcServerInterceptor.onHalfClose[cid=" + correlationId + "]");
                    trackState(correlationId, "HALF_CLOSE");
                    listener.onHalfClose();
                }

                public void onCancel() {
                    LOGGER.info("GrpcServerInterceptor.onCancel[cid=" + correlationId + "]");
                    final List<String> stateList = trackState(correlationId, "CANCEL");
                    listener.onCancel();
                    LOGGER.info(correlationId + " states: " + stateList);
                }

                public void onComplete() {
                    LOGGER.info("GrpcServerInterceptor.onComplete[cid=" + correlationId + "]");
                    final List<String> stateList = trackState(correlationId, "COMPLETE");
                    listener.onComplete();
                    LOGGER.info(correlationId + " states: " + stateList);
                }

                public void onReady() {
                    LOGGER.info("GrpcServerInterceptor.onReady[cid=" + correlationId + "]");
                    trackState(correlationId, "READY");
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