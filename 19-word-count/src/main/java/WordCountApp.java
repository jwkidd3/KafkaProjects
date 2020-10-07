import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

import java.util.Arrays;

public class WordCountApp {
    private static final Logger Logger = LogManager.getLogger();

    public static void main(final String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG,AppConfigs.stateStoreLocation);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> KS0 = streamsBuilder.stream(AppConfigs.topicName);

        KStream<String,String> KS1 = KS0.flatMapValues(v -> Arrays.asList(v.toLowerCase().split(" ")));


        KGroupedStream<String,String> KGS1 = KS1.groupBy((k,v) -> v);

        KTable<String,Long> KT1 = KGS1.count();

        KT1.toStream().print(Printed.<String,Long>toSysOut().withLabel("KT1"));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(),props);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            Logger.info("Shutting down stream");
            streams.close();;
        }
        ));




    }
}
