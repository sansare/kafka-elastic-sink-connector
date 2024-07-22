package com.skynyrd.kafka.transform;

import com.skynyrd.kafka.model.RecordSink;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.ParseException;
import java.util.Optional;

public interface RecordTransformer {
    Optional<RecordSink> apply(SinkRecord record) throws ParseException;
}
