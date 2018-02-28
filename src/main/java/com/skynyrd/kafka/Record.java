package com.skynyrd.kafka;

import javax.json.JsonObject;

public class Record {
    private final JsonObject doc;
    private final String id;

    public Record(JsonObject doc, String id) {
        this.doc = doc;
        this.id = id;
    }

    public JsonObject getDoc() {
        return doc;
    }

    public String getId() {
        return id;
    }
}
