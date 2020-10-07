package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {
    public static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        logger.info("Creating Kafka producer...");
        Properties props = new Properties();
        //purpose of the client_id is to track the source of the message
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        //comment seperated list of host/port pairs

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //since we send messages over a network they need to serialized

        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer, String>(props);

        logger.info("Starting to send messages...");
        for(int i = 0; i<AppConfigs.numEvents; i++){
            //Send method takes a ProducerRecord Object and sends it to the Kafka Cluster
            producer.send(new ProducerRecord<>(AppConfigs.topicName,i,"Simple string-" + i));
                    //takes 3 arguements, topicname, message key, message value
        }
        logger.info("Finished sending messsages");
        //last step is to close it
        producer.close();

    }
}
