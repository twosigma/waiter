#!/bin/sh

cat << EOF
static_resources:
  listeners:
  - address:
      socket_address:
        address: 0.0.0.0
        port_value: ${SERVICE_PORT}
    filter_chains:
    - filters:
      - name: envoy.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager
          codec_type: auto
          stat_prefix: http
          access_log:
            name: envoy.file_access_log
            typed_config:
              "@type": type.googleapis.com/envoy.config.accesslog.v2.FileAccessLog
              path: /dev/stdout
          route_config:
            name: reverse-proxy
            virtual_hosts:
            - name: backend
              domains:
              - "*"
              routes:
              - match:
                  prefix: "/"
                route:
                  cluster: waiter-app
          http_filters:
          - name: envoy.router
            typed_config: {}
  clusters:
  - name: waiter-app
    connect_timeout: 1s
    type: logical_dns
    dns_lookup_family: V4_ONLY
    lb_policy: round_robin
    load_assignment:
      cluster_name: waiter-app
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: 0.0.0.0
                port_value: ${PORT0}
admin:
  access_log_path: "/dev/stdout"
  address:
    socket_address:
      address: 0.0.0.0
      port_value: 15000
EOF