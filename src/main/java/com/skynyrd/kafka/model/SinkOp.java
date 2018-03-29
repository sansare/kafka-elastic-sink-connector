package com.skynyrd.kafka.model;

public enum SinkOp {
    CREATE("c"),
    UPDATE("u");

    private String op;

    SinkOp(String op) {
        this.op = op;
    }
}
