package com.skynyrd.kafka.client;

import com.skynyrd.kafka.model.Record;

import java.io.IOException;
import java.util.List;

public interface ElasticClient {
    void send(Record record, String type);

    void bulkSend(List<Record> records, String index, String type);

    void close() throws IOException;
}
