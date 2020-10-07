package com.kiddcorp.kafka;

import org.apache.kafka.common.protocol.types.Field;

class AppConfigs {
    final static String applicationID = "Multi-Threaded-Producer";
    final static String topicName = "nse-eod-topic";
    final static String kafkaConfigFileLocation = "kafka.properties";
    final static String[] eventfiles = {"data/NSE05NOV2018BHAV.csv", "data/NSE06NOV2018BHAV.csv"};
}
