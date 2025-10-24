package com.luminorgroup.poc.cdc.debezium.config;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.stream.Collectors;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "debezium")
public class DebeziumProperties {
    private String topicPrefix;
    private Map<String, String> config;
    private Map<String, String> producerConfig;
    public Map<String, String> getProperties() {
        return config;
    }

    @PostConstruct
    public void init() {
        this.topicPrefix = config.get("topic.prefix");
        this.producerConfig = config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("producer."))
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring("producer.".length()),
                        Map.Entry::getValue
                ));
    }

}
