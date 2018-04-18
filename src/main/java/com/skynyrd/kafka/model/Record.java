package com.skynyrd.kafka.model;

import com.google.gson.JsonObject;

public class Record {
    private final JsonObject doc;
    private final String id;
    private final RecordType type;
    private final String index;

    public Record(JsonObject doc, String id, RecordType type, String index) {
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
}
