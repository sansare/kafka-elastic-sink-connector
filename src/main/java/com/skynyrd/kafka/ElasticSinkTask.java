package com.skynyrd.kafka;

import com.skynyrd.kafka.service.ElasticService;
import com.skynyrd.kafka.service.ElasticServiceImpl;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ElasticSinkTask extends SinkTask {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private ElasticService elasticService;


    @Override
    public String version() {
        return VersionUtil.getVersion();
    }


    @Override
    public void start(Map<String, String> map) {
        elasticService = new ElasticServiceImpl(null, new ElasticSinkConnectorConfig(map));
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        try {
            elasticService.process(collection);
        }
        catch (Exception e) {
            log.error("Error while processing records");
            log.error(e.toString());
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        log.trace("Flushing the queue");
    }

    @Override
    public void stop() {
        try {
            elasticService.closeClient();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
