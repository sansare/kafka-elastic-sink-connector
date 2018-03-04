package com.skynyrd.kafka.model;

import com.google.gson.JsonObject;

public class Record {
    private final JsonObject doc;
    private final String id;
    private final RecordType type;

    public Record(JsonObject doc, String id, RecordType type) {
        this.doc = doc;
        this.id = id;
        this.type = type;
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
}
