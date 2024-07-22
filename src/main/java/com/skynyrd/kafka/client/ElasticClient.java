package com.skynyrd.kafka.client;
import com.skynyrd.kafka.model.RecordSink;

import java.io.IOException;

public interface ElasticClient {
    void send(RecordSink record, String type);

    // See implementation for details
    // void bulkSend(List<RecordSink> records, String index, String type);

    void close() throws IOException;
}
