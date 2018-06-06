package com.okmich.taxidata.service;

import java.math.BigDecimal;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawFareDataProcessingService implements Configuration {

    private static final Logger LOG = LoggerFactory.getLogger(RawFareDataProcessingService.class.getName());

    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "RawFareDataProcessingService");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        prop.put(ProducerConfig.ACKS_CONFIG, "all");

        StreamsBuilder builder = new StreamsBuilder();

        // get the data from kafka topic
        KStream<String, String> rawTripStream = builder
                .stream(Configuration.RAW_FARE_TOPIC);

        KStream<String, JsonNode> refinedTripStream = rawTripStream
                .map((String key, String value) -> {
                    JsonNode node = transform(value);
                    if (node == null) {
                        return new KeyValue("", null);
                    } else {
                        return new KeyValue(Configuration.getKey(value, 3), node);
                    }
                }).filter((Object key, Object value) -> !((String) key).isEmpty() && value != null
        );

        refinedTripStream.to(Configuration.REFINED_FARE_TOPIC,
                Produced.with(Serdes.String(), Configuration.JSON_SERDE));

        final Topology topology = builder.build();

        LOG.info(topology.toString());

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, prop);

        kafkaStreams
                .setUncaughtExceptionHandler((Thread t, Throwable e) -> {
                    e.printStackTrace(System.out);
                });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
        }));

        kafkaStreams.start();
    }

    public static JsonNode transform(String payload) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        String[] fields = payload.split(",");
        // medallion, hack_license, vendor_id, pickup_datetime, payment_type,
        // fare_amount, surcharge, mta_tax, tip_amount, tolls_amount,
        // total_amount
        try {

            node.put("medallion", fields[0]);
            node.put("hack_license", fields[1]);
            node.put("vendor_id", fields[2]);
            node.put("pickup_datetime", fields[3]);
            node.put("payment_type", fields[4]);
            node.put("fare_amount", new BigDecimal(fields[5]));
            node.put("surcharge", new BigDecimal(fields[6]));
            node.put("mta_tax", new BigDecimal(fields[7]));
            node.put("tip_amount", new BigDecimal(fields[8]));
            node.put("tolls_amount", new BigDecimal(fields[9]));
            node.put("total_amount", new BigDecimal(fields[10]));
        } catch (Exception ex) {
            return null;
        }
        return node;
    }

}
