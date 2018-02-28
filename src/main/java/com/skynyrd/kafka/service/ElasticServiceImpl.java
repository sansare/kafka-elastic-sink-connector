package com.skynyrd.kafka.service;

import com.google.gson.*;
import com.skynyrd.kafka.ElasticSinkConnectorConfig;
import com.skynyrd.kafka.Record;
import com.skynyrd.kafka.client.ElasticClient;
import com.skynyrd.kafka.client.ElasticClientImpl;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ElasticServiceImpl implements ElasticService {
    private static Logger log = LogManager.getLogger(ElasticServiceImpl.class);

    private static JsonConverter JSON_CONVERTER = new JsonConverter();
    static {
        JSON_CONVERTER.configure(Collections.singletonMap("schema", "false"), false);
    }

    private Gson gson;
    private String statusFlag;
    private String indexName;
    private String typeName;
    private ElasticClient elasticClient;
    private String dataListArrayName;

    public ElasticServiceImpl(ElasticClient elasticClient, ElasticSinkConnectorConfig config) {
        statusFlag = config.getFlagField();
        indexName = config.getIndexName();
        typeName = config.getTypeName();
        dataListArrayName = config.getDataListArrayField();

        PrepareJsonConverters();

        if(elasticClient == null) {
            try {
                elasticClient = new ElasticClientImpl(config.getElasticUrl(), config.getElasticPort());
            } catch (UnknownHostException e) {
                log.error("The host is unknown, exception stacktrace: " + e.toString());
            }
        }

        this.elasticClient = elasticClient;
    }

    @Override
    public void process(Collection<SinkRecord> recordsAsString){
        List<Record> recordList = new ArrayList<>();

        recordsAsString.forEach(record -> {
            try {
                JsonConverter converter = new JsonConverter();
                byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(
                        record.topic(), record.valueSchema(), record.value());
                String recordStr = new String(rawJsonPayload, StandardCharsets.UTF_8);
                JsonObject recordAsJson = gson.fromJson(recordStr, JsonObject.class);

                JsonObject payload = recordAsJson.getAsJsonObject("payload").getAsJsonObject("after");

                log.error("PAYLOAD: " + payload);

                recordList.add(new Record(payload, ""));
            } catch (JsonSyntaxException e) {
                log.error("Cannot deserialize json string, which is : " + record);
            } catch (Exception e) {
                log.error("Cannot process data, which is : " + record);
                log.error("EXC: ", e);
            }
        });

        try {
            elasticClient.bulkSend(recordList, indexName, typeName);
        }
        catch (Exception e) {
            log.error("Something failed, here is the error:");
            log.error(e.toString());
        }
    }

    @Override
    public void closeClient() throws IOException {
        elasticClient.close();
    }

    private void PrepareJsonConverters() {
        JsonConverter converter = new JsonConverter();
        converter.configure(Collections.singletonMap("schemas.enable", "false"), false);

        gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();
    }
}
