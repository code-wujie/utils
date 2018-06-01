


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Created by WJ on 2017/11/13.
 */
public class Kafka_Producer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.200.21:19092,192.168.200.22:19092");
        properties.put("metadata.broker.list", "192.168.200.21:19092,192.168.200.22:19092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("request.required.acks", "1");

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
        for (int iCount = 0; iCount < 100; iCount++) {
            String message = "My Test Message No " + iCount;
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("task3", message);
            producer.send(record);
        }
        producer.close();
    }

}
