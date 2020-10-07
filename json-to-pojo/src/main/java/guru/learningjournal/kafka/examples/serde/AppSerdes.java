package guru.learningjournal.kafka.examples.serde;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;
import guru.learningjournal.kafka.examples.types.DeliveryAddress

public class AppSerdes extends Serdes {

    static public final class PosInvoiceSerde extends WrapperSerde<PosInvoice>{
        public PosInvoiceSerde(){
            super(new JsonSerializer<>(), new JsonDeserializer<>());

        }
    }

    static public Serde<PosInvoice> PosInvoice(){
        PosInvoiceSerde serde = new PosInvoiceSerde();
        Map<String,Object> serdeConfigs = new HashMap<>();
        serde.configure(serdeConfigs,false);

        return serde;
    }


    static public final class DeliveryAddressSerde extends WrapperSerde<DeliveryAddress>{
        public DeliveryAddressSerde(){
            super(new JsonSerializer<>(), new JsonDeserializer<>());

        }
    }

    static public Serde<DeliveryAddress> DeliveryAddress(){
        DeliveryAddressSerde serde = new DeliveryAddressSerde();
        Map<String,Object> serdeConfigs = new HashMap<>();
        serde.configure(serdeConfigs,false);

        return serde;
    }


}
