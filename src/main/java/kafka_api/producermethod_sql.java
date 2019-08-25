package kafka_api;

import java.util.*;
import java.io.*;
import org.apache.kafka.clients.producer.*;


public class producermethod_sql {

    public static void producestr (String s) {
        String topicName = "testTopicsql";
        String key = "Key1";
        //String value = "Value-1";
        String value = s;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);

        ProducerRecord<String, String> record = new ProducerRecord(topicName, key, value);
        producer.send(record);

        producer.close();
    }
}
