package com.kafkamgt.clusterapi.models;

public enum KafkaClustersType {
    kafka("kafka"),
    schemaregistry("schemaregistry"),
    kafkaconnect("kafkaconnect");

    public final String value;
    private KafkaClustersType(String value) {
        this.value = value;
    }
}
