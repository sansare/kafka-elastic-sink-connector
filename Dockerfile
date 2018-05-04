FROM confluentinc/cp-kafka-connect:4.0.0

ENV CONNECT_PLUGIN_PATH="/etc/kafka-connect/plugins"

RUN mkdir -p /etc/kafka-connect/plugins
COPY kc-plugins /etc/kafka-connect/plugins
