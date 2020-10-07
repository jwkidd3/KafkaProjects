package guru.learningjournal.kafka.examples;

class AppConfigs {
    final static String applicationID = "HelloProducer";
    final static String bootstrapServers = "localhost:9092,localhost:9093";
    final static String topicName = "hello-producer-topic-1";
    final static int numEvents = 1000000;
}
