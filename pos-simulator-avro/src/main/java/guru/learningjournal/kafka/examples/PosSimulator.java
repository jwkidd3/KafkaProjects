package guru.learningjournal.kafka.examples;


import guru.learningjournal.kafka.examples.types.PosInvoice;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PosSimulator {
    private static final Logger logger = LogManager.getLogger();


    public static void main(String[] args) {
        if(args.length < 3){
            System.out.println("Please supply 3 arguments");
            System.exit(-1);
        }

        String topicName = args[0];
        int num_of_threads = new Integer(args[1]);
        int produce_speed = new Integer(args[2]);


        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppsConfig.applicationId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppsConfig.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppsConfig.schemaRegistryServers);

        KafkaProducer<String, PosInvoice> kafkaProducer = new KafkaProducer<String, PosInvoice>(props);
        ExecutorService executor = Executors.newFixedThreadPool(num_of_threads);

        final List<RunnableProducer> runnableProducers = new ArrayList<>();

        for(int i = 0; i < num_of_threads; i++){
            RunnableProducer runnableProducer = new RunnableProducer(kafkaProducer,topicName,produce_speed,i);
            runnableProducers.add(runnableProducer);
            executor.submit(runnableProducer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (RunnableProducer p : runnableProducers) p.shutdown();
            executor.shutdown();
            logger.info("Closing Executor Service");
            try {
                executor.awaitTermination(produce_speed * 2, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));




    }

}
