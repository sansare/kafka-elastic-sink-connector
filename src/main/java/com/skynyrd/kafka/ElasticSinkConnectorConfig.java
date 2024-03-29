package com.skynyrd.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class ElasticSinkConnectorConfig extends AbstractConfig {
    public static final String TYPE_NAME = "type.name";
    private static final String TYPE_NAME_DOC = "Type of the Elastic index you want to work on.";
    public static final String ELASTIC_URL = "elastic.url";
    private static final String ELASTIC_URL_DOC = "Elastic URL to connect.";
    public static final String ELASTIC_PORT = "elastic.port";
    private static final String ELASTIC_PORT_DOC = "Elastic transport port to connect.";

    public ElasticSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public ElasticSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(TYPE_NAME, Type.STRING, Importance.HIGH, TYPE_NAME_DOC)
                .define(ELASTIC_URL, Type.STRING, Importance.HIGH, ELASTIC_URL_DOC)
                .define(ELASTIC_PORT, Type.INT, Importance.HIGH, ELASTIC_PORT_DOC);
    }

    public String getTypeName() {
        return this.getString(TYPE_NAME);
    }

    public String getElasticUrl(){
        return this.getString(ELASTIC_URL);
    }

    public Integer getElasticPort() {
        return this.getInt(ELASTIC_PORT);
    }
}
