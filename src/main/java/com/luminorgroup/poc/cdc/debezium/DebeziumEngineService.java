package com.luminorgroup.poc.cdc.debezium;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luminorgroup.poc.cdc.debezium.config.DebeziumProperties;
import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.spi.OffsetCommitPolicy;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class DebeziumEngineService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DebeziumProperties debeziumProperties;
    private final KafkaProducerService producerService;
    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;

    public void start() {

        if (debeziumProperties == null || debeziumProperties.getProperties() == null || debeziumProperties.getProperties().isEmpty()) {
            log.error("start:: Debezium Engine Service Configuration is null or empty!");
            return;
        }

        log.error("start:: Debezium Engine Service Configuration:");
        debeziumProperties.getProperties().forEach(
              (key, value) -> log.info("start::   {} == {}", key, value)
        );


        Configuration config = Configuration.from(debeziumProperties.getProperties());

        log.info("start:: Building Debezium engine...");

        try {
            engine = DebeziumEngine.create(Json.class)
                    .using(config.asProperties())
                    .using(OffsetCommitPolicy.always())
                    .notifying(this::handleChangeEvent)
                    .build();

            log.info("start:: Building Debezium engine complete.");
        } catch (Exception e) {
            log.error("start:: Error building Debezium engine.", e);
        }

        executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            try {
                log.info("start:: Starting Debezium engine...");
                engine.run();
            } catch (Exception e) {
                log.error("start:: Error running Debezium engine", e);
            }
        });

        log.info("start:: Debezium Engine started successfully");
    }

    @PreDestroy
    public void stop() {
        log.info("stop:: Stopping Debezium engine...");

        if (engine != null) {
            try {
                engine.close();
            } catch (IOException e) {
                log.error("stop:: Error closing Debezium engine", e);
            }
        }

        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        log.info("stop:: Debezium engine stopped");
    }

    private void handleChangeEvent(ChangeEvent<String, String> event) {
        try {
            log.debug("handleChangeEvent:: Received change event");

            if (event.value() != null) {
                log.debug("handleChangeEvent:: Change data event value: {}", event.value());
                producerService.send(createCdcMessage(event));
            }

            if (event.key() != null) {
                log.debug("handleChangeEvent:: Change event key: {}", event.key());
            }

            if (event.key() == null && event.value() == null) {
                log.warn("handleChangeEvent:: Received change event's key and value are null '{}'", event);
            }

        } catch (Exception e) {
            log.error("handleChangeEvent:: Error handling change event", e);
        }
    }


    private <K, V> CDCMessage<K, V> createCdcMessage(ChangeEvent<String, String> event) throws JsonProcessingException {
        JsonNode eventValue = objectMapper.readTree(event.value());

        String topicPrefix = debeziumProperties.getTopicPrefix();
        String topicSchema = "UNKNOWN-SCHEMA";
        String topicTable = "UNKNOWN-TABLE";

        if (eventValue.has("source")) {
            JsonNode source = eventValue.get("source");
            log.trace("createCdcMessage:: source = '{}'", source);

            if (eventValue.has("name")) {
                topicPrefix = eventValue.get("name").asText(topicPrefix);
                log.trace("createCdcMessage:: source.name = '{}'", topicPrefix);
            }

            if (eventValue.has("schema")) {
                topicSchema = eventValue.get("schema").asText(topicSchema);
                log.trace("createCdcMessage:: source.schema = '{}'", topicSchema);
            }

            if (eventValue.has("table")) {
                topicTable = eventValue.get("table").asText(topicTable);
                log.trace("createCdcMessage:: source.table = '{}'", topicTable);
            }
        }

        String topic = topicPrefix + '.' + topicSchema + '.' +topicTable;

        String payload = "UNKNOWN-PAYLOAD";
        if (eventValue.has("payload")) {
            JsonNode payloadNode = eventValue.get("payload");
            log.trace("createCdcMessage:: payloadNode = '{}'", payloadNode);

            payload = payloadNode.asText(payload);
            log.trace("createCdcMessage:: payload = '{}'", payload);
        }
        return new CDCMessage<>(topic, event.key(), payload);
    }

}
