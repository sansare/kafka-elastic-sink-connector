FROM alpine:latest

COPY kc-plugins /data/kc-plugins
VOLUME /data/kc-plugins
