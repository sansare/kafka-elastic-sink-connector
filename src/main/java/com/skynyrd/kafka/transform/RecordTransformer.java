package com.skynyrd.kafka.transform;

import com.skynyrd.kafka.Record;
import org.apache.kafka.connect.sink.SinkRecord;

import java.text.ParseException;

public interface RecordTransformer {
    Record apply(SinkRecord record) throws ParseException;
}
