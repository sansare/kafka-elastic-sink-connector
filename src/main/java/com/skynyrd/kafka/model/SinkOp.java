package com.skynyrd.kafka.model;

public enum SinkOp {
    CREATE("c"),
    UPDATE("u"),
    DELETE("d"),
    DB_SOFT_DELETE("dbsd"),
    UNKNOWN("");

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
            case "d":
                return DELETE;
            default:
                return UNKNOWN;
        }
    }
}
