package com.skynyrd.kafka.model;

import com.google.gson.JsonObject;

import java.util.Optional;

public class SinkPayload {
    private SinkOp op;
    private Optional<JsonObject> before;
    private Optional<JsonObject> after;

    public SinkPayload(SinkOp op, Optional<JsonObject> before, Optional<JsonObject> after) {
        this.op = op;
        this.before = before;
        this.after = after;
    }

    public SinkOp getOp() {
        return op;
    }

    public Optional<JsonObject> getBefore() {
        return before;
    }

    public Optional<JsonObject> getAfter() {
        return after;
    }
}
