package com.skynyrd.kafka;

import com.google.gson.JsonObject;

public class Record {
    private final JsonObject dataObject;
    private final String behaviour;

    public Record(JsonObject dataObject, String behaviour) {
        this.dataObject = dataObject;
        this.behaviour = behaviour;
    }

    public JsonObject getDataObject() {
        return dataObject;
    }

    public String getBehaviour() {
        return behaviour;
    }
}
