package com.kiddcorp.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class DispatcherDemo {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        try{
            InputStream inputStream = new FileInputStream(AppConfigs.kafkaConfigFileLocation);
            props.load(inputStream);
            props.put(ProducerConfig.CLIENT_ID_CONFIG,AppConfigs.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        }
        catch (IOException e){
            throw new RuntimeException(e);
        }

        KafkaProducer<Integer,String> kafkaProducer = new KafkaProducer<Integer, String>(props);
        Thread[] dispatchers = new Thread[AppConfigs.eventfiles.length];
        logger.info("Starting dispatcher....");
        for(int i=0; i < AppConfigs.eventfiles.length; i++){
            dispatchers[i] = new Thread(new Dispatcher(kafkaProducer,AppConfigs.topicName,AppConfigs.eventfiles[i]));
            dispatchers[i].start();
        }
        try{
            for(Thread t: dispatchers) t.join();
        }
        catch (Exception e){
            logger.error("Main thread interuppted");
        }
        finally {
            kafkaProducer.close();
        }

    }
}
