package com.kiddcorp.kafka;
import java.io.File;
import java.util.Scanner;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
public class Dispatcher implements Runnable {
    private static final Logger logger = LogManager.getLogger();
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer,String> producer;

    Dispatcher(KafkaProducer<Integer,String> producer, String topicName,String fileLocation){
        this.producer = producer;
        this.topicName = topicName;
        this.fileLocation = fileLocation;
    }


    @Override
    public void run() {
        logger.info("Starting processing" + fileLocation);
        File file = new File(this.fileLocation);
        try{
            Scanner myReader = new Scanner(file);
            while(myReader.hasNextLine()){
                String data = myReader.nextLine();
                this.producer.send(new ProducerRecord<>(this.topicName,null,data));
            }
            logger.info("Finished sending messsages");

        }
        catch(Exception e){
            throw new RuntimeException(e);
        }


    }
}
