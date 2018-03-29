package com.skynyrd.kafka.model;

public enum SinkOp {
    CREATE("c"),
    UPDATE("u"),
    DELETE("d");

    private String op;

    SinkOp(String op) {
        this.op = op;
    }

    public static SinkOp fromStr(String input) {
        switch (input) {
            case "c":
                return CREATE;
            case "u":
                return UPDATE;
            default:
                return DELETE;
        }
    }
}
