package com.exapmle.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger =  LoggerFactory.getLogger(ProducerDemoKeys.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
            String topic = "TestTopic";
            String value = "hi"+i;
            String key ="id_"+i;
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,value);
            logger.info("key: "+key);

            //the key is always go to the same partition even if we run multiple times for fixed no.of partitions
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //record send
                        logger.info("Received new metadata. \n" + "Topic:" + recordMetadata.topic() + "\n" + "Partition:" + recordMetadata.partition() + "\n" + "Offset:" + recordMetadata.offset() + "\n" + "Timestamp" + recordMetadata.timestamp());
                    } else {
                        logger.error("exception is", e);
                    }
                }
            }).get();    //block to .send() to make it synchronous
        }
       // producer.send(record1);
        producer.flush();
        producer.close();
    }
}

