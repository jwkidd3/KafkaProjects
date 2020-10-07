package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RewardsPartitioner implements StreamPartitioner<String, PosInvoice> {


    @Override
    public Integer partition(String topic, String key, PosInvoice value, int numPartitions) {
        return value.getCustomerCardNo().hashCode() % numPartitions;

    }
}
