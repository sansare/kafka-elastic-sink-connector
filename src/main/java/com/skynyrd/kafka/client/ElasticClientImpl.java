package com.skynyrd.kafka.client;

import com.skynyrd.kafka.model.Record;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

public class ElasticClientImpl implements ElasticClient {
    private JestClient client;
    private static Logger log = LogManager.getLogger(ElasticClientImpl.class);

    public ElasticClientImpl(String url, int port) throws UnknownHostException {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(String.format("http://%s:%s", url, port))
                .multiThreaded(true)
                .build());

        client = factory.getObject();
    }

    public void send(Record record, String type) {
        try {
            switch (record.getType()) {
                case INSERT:
                    log.info("Sending INDEX record" + record.toString());
                    client.execute(
                            new Index.Builder(record.getDoc())
                                    .index(record.getIndex())
                                    .id(record.getId())
                                    .type(type)
                                    .build());
                    break;
                case UPDATE:
                    log.info("Sending UPDATE record" + record.toString());
                    client.execute(
                            new Update.Builder(record.getDoc())
                                    .index(record.getIndex())
                                    .id(record.getId())
                                    .type(type)
                                    .build());
                    break;
                case DELETE:
                    log.info("Sending DELETE record" + record.toString());
                    client.execute(
                            new Delete.Builder(record.getId())
                                    .index(record.getIndex())
                                    .type(type)
                                    .build()).getErrorMessage();
                default:
                    log.info("Operation not supported");
            }
        } catch (IOException e) {
            log.error(e.toString());
            e.printStackTrace();
        }
    }

    // ATTENTION: use with care, documents can be written to Elastic in a wrong order. Left for reference purposes.

//    public void bulkSend(List<Record> records, String index, String type) {
//        Bulk.Builder indexBuilder = new Bulk.Builder()
//                .defaultIndex(index)
//                .defaultType(type);
//        Bulk.Builder updateBuilder = new Bulk.Builder()
//                .defaultIndex(index)
//                .defaultType(type);
//
//        boolean indexPresent = false;
//        boolean updatePresent = false;
//        for (Record record : records) {
//            switch (record.getType()) {
//                case UPDATE:
//                    updateBuilder.addAction(new Update.Builder(record.getDoc()).id(record.getId()).build());
//                    updatePresent = true;
//                    break;
//                default:
//                    indexBuilder.addAction(new Index.Builder(record.getDoc()).id(record.getId()).build());
//                    indexPresent = true;
//            }
//
//        }
//
//        if (indexPresent) {
//            bulkExecute(indexBuilder.build());
//        }
//        if (updatePresent) {
//            bulkExecute(updateBuilder.build());
//        }
//    }
//
//    private void bulkExecute(Bulk bulk) {
//        try {
//            BulkResult bulkResult = client.execute(bulk);
//            String errorMessage = bulkResult.getErrorMessage();
//
//            if (errorMessage != null) {
//                bulkResult.getFailedItems().forEach(bulkResultItem -> {
//                    log.error("BULK: " + bulkResultItem.errorReason);
//                    log.error(bulkResultItem.error);
//                });
//                log.error(errorMessage);
//            }
//        } catch (IOException e) {
//            log.error(e.toString());
//            e.printStackTrace();
//        }
//    }

    public void close() throws IOException {
        client.shutdownClient();
    }
}
