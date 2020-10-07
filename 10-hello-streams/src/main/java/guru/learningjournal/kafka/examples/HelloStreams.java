package guru.learningjournal.kafka.examples;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.internals.KStreamTransformValues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class HelloStreams {
    private final static Logger logger = LogManager.getLogger();


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());


        StreamsBuilder streamsBuilder =  new StreamsBuilder();
        KStream<Integer,String> kStream = streamsBuilder.stream(AppConfigs.topicName);
        kStream.foreach((k,v) -> System.out.println("Keys= " + k + "Values= " + v));

        //kStream.peek((k,v) -> System.out.println("Keys= " + k + "Values= " + v));
        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        logger.info("Starting stream");
        streams.start();


        Runtime.getRuntime().addShutdownHook(new Thread( () ->{
            logger.info("Shutting down streams");
            streams.close();
        }
        ));



    }
}
