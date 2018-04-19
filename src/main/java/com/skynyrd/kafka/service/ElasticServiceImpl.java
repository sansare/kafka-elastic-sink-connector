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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticServiceImpl implements ElasticService {
    private static Logger log = LogManager.getLogger(ElasticServiceImpl.class);

    private static final Pattern TABLE_PATTERN =
            Pattern.compile("Struct\\{.*table=(.+)\\}");

    private String typeName;
    private ElasticClient elasticClient;

    public ElasticServiceImpl(ElasticClient elasticClient, ElasticSinkConnectorConfig config) {
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
    public void process(Collection<SinkRecord> records) {
        records.forEach(record -> {
            log.info("Record received: " + record.toString());

            try {
                Optional<String> tableOpt = extractTable(record);

                tableOpt.ifPresent(table -> {
                    Optional<AbstractRecordTransformer> recordTransformer =
                            RecordTransformerFactory.getTransformer(table);

                    recordTransformer.ifPresent(transformer -> {
                        try {
                            Optional<Record> transformedRecord = transformer.apply(record);
                            transformedRecord.ifPresent(rec -> elasticClient.send(rec, typeName));
                        } catch (Exception e) {
                            log.error("Error processing record", e);
                        }
                    });
                });
            } catch (Exception e) {
                log.error(e);
            }
        });
    }

    private Optional<String> extractTable(SinkRecord record) {
        Matcher matcher = TABLE_PATTERN.matcher(record.key().toString());

        if (matcher.find()) {
            String table = matcher.group(1);
            log.info("Extracted table: [" + table + "]");
            return Optional.of(table);
        } else {
            log.info("No table found in key " + record.key());
            return Optional.empty();
        }
    }

    @Override
    public void closeClient() throws IOException {
        elasticClient.close();
    }
}
