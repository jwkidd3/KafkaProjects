package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.datagenerator.InvoiceGenerator;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class RunnableProducer implements Runnable {

    private static final Logger logger = LogManager.getLogger();
    private KafkaProducer<String, PosInvoice> producer;
    private String topicName;
    private int produce_speed;
    private InvoiceGenerator invoiceGenerator;
    private int id;
    private final AtomicBoolean stopper = new AtomicBoolean(false);

    RunnableProducer(KafkaProducer<String, PosInvoice> producer, String topicName, int produce_speed, int id){
        this.id = id;
        this.producer = producer;
        this.topicName = topicName;
        this.produce_speed = produce_speed;
        this.invoiceGenerator = InvoiceGenerator.getInstance();

    }
    @Override
    public void run() {

        try{
            logger.info("Starting a producer thread - " + id );
            while(!stopper.get()) {
                PosInvoice posInvoice = invoiceGenerator.getNextInvoice();
                producer.send(new ProducerRecord<>(topicName, posInvoice.getStoreID().toString(),posInvoice));
                Thread.sleep(produce_speed);
            }
        }
        catch(Exception e){
            logger.error("Exception in Producer thread - " + id);
            throw new RuntimeException(e);
        }

    }

    public void shutdown(){
        logger.info("Shutting down producer -" + id);
        stopper.set(true);
    }
}
