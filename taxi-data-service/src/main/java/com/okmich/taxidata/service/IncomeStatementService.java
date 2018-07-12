package com.okmich.taxidata.service;

import static com.okmich.taxidata.service.Configuration.JSON_SERDE;
import static com.okmich.taxidata.service.Configuration.KAFKA_BROKER;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

public class IncomeStatementService {

    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "IncomeStatementService");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        prop.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSON_SERDE.getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //to do
        
        //configure kafka stream
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), prop);
        kafkaStreams
                .setUncaughtExceptionHandler((Thread t, Throwable e) -> {
                    e.printStackTrace(System.out);
                });

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
        }));

    }

}
