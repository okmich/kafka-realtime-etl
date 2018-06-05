package com.okmich.taxidata.service;

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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.text.ParseException;

public class RawTripDataProcessingService implements Configuration {

    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "RawTripDataProcessingService");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // get the data from kafka topic
        KStream<String, String> rawTripStream = builder
                .stream(Configuration.RAW_TRIP_TOPIC);

        KStream<String, JsonNode> refinedTripStream = rawTripStream
                .map((String key, String value) -> {
                    JsonNode node = transform(value);
                    if (node == null) {
                        return new KeyValue("", null);
                    } else {
                        return new KeyValue(Configuration.getKey(value, 5), node);
                    }
                }).filter((Object key, Object value) -> !((String) key).isEmpty() && value != null);

        //send to sink
        refinedTripStream.to(REFINED_TRIP_TOPIC, Produced.with(Serdes.String(), JSON_SERDE));

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),
                prop);

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
        // medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag,
        // pickup_datetime, dropoff_datetime, passenger_count,
        // trip_time_in_secs, trip_distance, pickup_longitude, pickup_latitude,
        // dropoff_longitude, dropoff_latitude
        try {

            long pickupDT = DATE_FORMATTER.parse(fields[5]).getTime();
            long dropoffDT = DATE_FORMATTER.parse(fields[6]).getTime();

            long durationInSeconds = (dropoffDT - pickupDT) / 1000;

            node.put("medallion", fields[0]);
            node.put("hack_license", fields[1]);
            node.put("vendor_id", fields[2]);
            node.put("rate_code", fields[3]);
            node.put("store_and_fwd_flag", fields[4]);
            node.put("pickup_datetime", fields[5]);
            node.put("dropoff_datetime", fields[6]);
            node.put("passenger_count", Integer.parseInt(fields[7]));
            node.put("trip_time_in_secs", durationInSeconds);
            node.put("trip_distance", Float.parseFloat(fields[9]));
            ArrayNode pickupNode = node.putArray("pickup_location");
            pickupNode.add(Float.parseFloat(fields[10]));
            pickupNode.add(Float.parseFloat(fields[11]));
            ArrayNode dropOffNode = node.putArray("dropoff_location");
            if (fields.length > 12) {
                dropOffNode.add(Float.parseFloat(fields[12]));
                dropOffNode.add(Float.parseFloat(fields[13]));
            }
        } catch (NumberFormatException | ParseException ex) {
            return null;
        }

        return node;
    }

}
