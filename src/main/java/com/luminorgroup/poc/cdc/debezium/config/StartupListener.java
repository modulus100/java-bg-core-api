package com.luminorgroup.poc.cdc.debezium.config;

import com.luminorgroup.poc.cdc.debezium.DebeziumEngineService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StartupListener {

    private final DebeziumEngineService debeziumEngineService;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("=====================================================");
        log.info("onApplicationReady::  -- START --                    ");
        log.info("onApplicationReady:: Debezium Service                ");
        log.info("=====================================================");

        debeziumEngineService.start();
    }
}
