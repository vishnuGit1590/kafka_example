package com.exapmle.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallBack {
    public static void main(String[] args) {
        Logger logger =  LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
            //create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("TestTopic", "helloworld" + i);
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
            });
        }
       // producer.send(record1);
        producer.flush();
        producer.close();
    }
}

