package com.skynyrd.kafka.model;

import com.google.gson.JsonObject;

import java.util.Optional;

public class SinkPayload {
    private SinkOp op;
    private Optional<JsonObject> payload;

    public SinkPayload(SinkOp op, Optional<JsonObject> payload) {
        this.op = op;
        this.payload = payload;
    }

    public SinkOp getOp() {
        return op;
    }

    public Optional<JsonObject> getPayload() {
        return payload;
    }
}
