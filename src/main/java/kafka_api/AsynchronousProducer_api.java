package kafka_api;



import java.util.*;
import java.io.*;
import org.apache.kafka.clients.producer.*;


public class AsynchronousProducer_api {

    public static void main(String[] args) throws Exception {

        File file = new File("/home/kishan/test_producer.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        while ((st = br.readLine()) != null) {
            System.out.println(st);

            String topicName = "testTopicXYZ";
            String key = "Key1";
            //String value = "Value-1";
            String value = st;

            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer(props);

            ProducerRecord<String, String> record = new ProducerRecord(topicName, key, value);

            producer.send(record, new MyProducerCallback());
            System.out.println("AsynchronousProducer call completed");
            producer.close();
        }
    }
}

class MyProducerCallback implements Callback{

    @Override
    public  void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null)
            System.out.println("AsynchronousProducer failed with an exception");
        else
            System.out.println("AsynchronousProducer call Success:");
    }
}


/*
class MyProducerCallback implements Callback{

    @Override
    public  void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null)
            System.out.println("AsynchronousProducer failed with an exception");
        else
            System.out.println("AsynchronousProducer call Success:");
    }
}
*/
