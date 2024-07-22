package com.skynyrd.kafka.model;

import com.google.gson.JsonObject;

public class RecordSink {
    private final JsonObject doc;
    private final String id;
    private final RecordType type;
    private final String index;

    public RecordSink(JsonObject doc, String id, RecordType type, String index) {
        this.doc = doc;
        this.id = id;
        this.type = type;
        this.index = index;
    }

    public JsonObject getDoc() {
        return doc;
    }

    public String getId() {
        return id;
    }

    public RecordType getType() {
        return type;
    }

    public String getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "RecordSink{" +
                "doc=" + doc +
                ", id='" + id + '\'' +
                ", type=" + type +
                ", index='" + index + '\'' +
                '}';
    }
}
