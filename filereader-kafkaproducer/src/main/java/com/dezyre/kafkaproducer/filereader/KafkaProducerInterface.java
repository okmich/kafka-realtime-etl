/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.kafkaproducer.filereader;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author m.enudi
 */
public class KafkaProducerInterface {

    private final String topic;
    private final Producer<String, String> producer;

    public KafkaProducerInterface(String brokerUrl, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerUrl);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String message, long eventTs) {
//        ProducerRecord<String, String> producerRecord = new ProducerRecord(this.topic,0, eventTs, "", message);
        ProducerRecord<String, String> producerRecord = new ProducerRecord(this.topic, "", message);
        producer.send(producerRecord);  
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}
