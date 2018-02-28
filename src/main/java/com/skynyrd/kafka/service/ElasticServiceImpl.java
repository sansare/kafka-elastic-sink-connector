package com.skynyrd.kafka.service;

import com.skynyrd.kafka.ElasticSinkConnectorConfig;
import com.skynyrd.kafka.model.Record;
import com.skynyrd.kafka.client.ElasticClient;
import com.skynyrd.kafka.client.ElasticClientImpl;
import com.skynyrd.kafka.transform.AbstractRecordTransformer;
import com.skynyrd.kafka.transform.RecordTransformerFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

public class ElasticServiceImpl implements ElasticService {
    private static Logger log = LogManager.getLogger(ElasticServiceImpl.class);

    private String indexName;
    private String typeName;
    private ElasticClient elasticClient;

    public ElasticServiceImpl(ElasticClient elasticClient, ElasticSinkConnectorConfig config) {
        indexName = config.getIndexName();
        typeName = config.getTypeName();

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
                AbstractRecordTransformer recordTransformer = RecordTransformerFactory.getTransformer(record.topic());
                recordList.add(recordTransformer.apply(record));
            } catch (Exception e) {
                log.error("Error processing record", e);
            }
        });

        try {
            elasticClient.bulkSend(recordList, indexName, typeName);
        }
        catch (Exception e) {
            log.error("Error sending to Elastic", e);
        }
    }

    @Override
    public void closeClient() throws IOException {
        elasticClient.close();
    }
}
