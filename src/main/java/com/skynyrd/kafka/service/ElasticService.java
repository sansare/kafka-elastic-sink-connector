package com.skynyrd.kafka.service;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.Collection;

public interface ElasticService {
    void process(Collection<SinkRecord> recordsAsString);
    void closeClient() throws IOException;
}
