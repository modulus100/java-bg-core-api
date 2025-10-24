package com.luminorgroup.poc.cdc.debezium;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class CDCMessage<K, V> {
    private final String topic;
    private final String schema;
    private final String table;
    private K key;
    private V value;
}
