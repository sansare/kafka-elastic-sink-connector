package com.skynyrd.kafka.model;

import com.google.gson.JsonObject;

import java.util.Optional;

public class SinkPayload {
    private Optional<JsonObject> before;
    private Optional<JsonObject> after;

    public SinkPayload(Optional<JsonObject> before, Optional<JsonObject> after) {
        this.before = before;
        this.after = after;
    }

    public Optional<JsonObject> getBefore() {
        return before;
    }

    public Optional<JsonObject> getAfter() {
        return after;
    }
}
