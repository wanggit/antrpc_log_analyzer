package io.github.wanggit.antrpc.log.analyzer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class AntrpcLogMessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AntrpcLogMessageConsumer.class);

    private static final String INDEX_NAME = "call_logs";

    @Autowired private RestHighLevelClient highLevelClient;

    @PostConstruct
    public void init() {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
        createIndexRequest.settings(
                Settings.builder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 2));
        highLevelClient
                .indices()
                .createAsync(
                        createIndexRequest,
                        RequestOptions.DEFAULT,
                        new ActionListener<CreateIndexResponse>() {
                            @Override
                            public void onResponse(CreateIndexResponse createIndexResponse) {
                                if (logger.isInfoEnabled()) {
                                    logger.info("create index " + createIndexResponse.index());
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("create elasticsearch index error.");
                                }
                            }
                        });
    }

    @KafkaListener(topics = "antrpc_call_log")
    public void consume(ConsumerRecord<String, String> record) {
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME);
        indexRequest.source(record.value(), XContentType.JSON);
        highLevelClient.indexAsync(
                indexRequest,
                RequestOptions.DEFAULT,
                new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("saved to " + indexResponse.status().getStatus());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("save log to elasticsearch error.", e);
                        }
                    }
                });
    }
}
