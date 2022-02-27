package com.skynyrd.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class ElasticSinkConnectorConfig extends AbstractConfig {
    public static final String TYPE_NAME = "type.name";
    private static final String TYPE_NAME_DOC = "Type of the Elastic index you want to work on.";
    public static final String CONNECTION_URL = "connection.url";
    private static final String CONNECTION_URL_DOC = "Elastic URL to connect.";

    public ElasticSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public ElasticSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(TYPE_NAME, Type.STRING, Importance.HIGH, TYPE_NAME_DOC)
                .define(CONNECTION_URL, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC);
//                .define("key.converter", Type.STRING, Importance.HIGH, "key.converter")
//                .define("value.converter", Type.STRING, Importance.HIGH, "value.converter")
//                .define("tasks.max", Type.STRING, Importance.HIGH, "tasks.max")
//                .define("topics", Type.STRING, Importance.HIGH, "topics");
    }

    public String getTypeName() {
        return this.getString(TYPE_NAME);
    }

    public String getElasticUrl() {
        return this.getString(CONNECTION_URL);
    }
}
