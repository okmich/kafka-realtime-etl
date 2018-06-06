package com.okmich.taxidata.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class TripFareServiceUsingStream implements Configuration {

    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "TripFareService");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSON_SERDE.getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // get the refined json data from kafka topic as a table
        KStream<String, JsonNode> refinedTripTable = builder.stream(
                REFINED_TRIP_TOPIC, Consumed.with(Serdes.String(), JSON_SERDE));

        // get the refined fares json data from kafka topic
        KStream<String, JsonNode> refinedFaresTable = builder
                .stream(REFINED_FARE_TOPIC, Consumed.with(Serdes.String(), JSON_SERDE));

        //join both streams
        KStream<String, JsonNode> joinedTable
                = refinedFaresTable.leftJoin(refinedTripTable, (JsonNode tripNode, JsonNode fareNode) -> {
                    return transform(tripNode, fareNode);
                }, JoinWindows.of(600000));

        //load to sink
        joinedTable.to(MERGED_TRIP_FARES_TOPIC, Produced.with(Serdes.String(), JSON_SERDE));

        //build kafka streams
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

    public static JsonNode transform(JsonNode tripNode, JsonNode fareNode) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        node.put("medallion", fareNode.get("medallion").asText());
        node.put("hack_license", fareNode.get("hack_license").asText());
        node.put("vendor_id", ifNotEmpty(fareNode.get("vendor_id")));
        node.put("payment_type", ifNotEmpty(fareNode.get("payment_type")));
        node.put("fare_amount", fareNode.get("fare_amount").asDouble(0));
        node.put("surcharge", fareNode.get("surcharge").asDouble(0));
        node.put("mta_tax", fareNode.get("mta_tax").asDouble(0));
        node.put("tip_amount", fareNode.get("tip_amount").asDouble(0));
        node.put("tolls_amount", fareNode.get("tolls_amount").asDouble(0));
        node.put("total_amount", fareNode.get("total_amount").asDouble(0));

        node.put("rate_code", ifNotEmpty(tripNode.get("rate_code")));
        node.put("store_and_fwd_flag", ifNotEmpty(tripNode.get("store_and_fwd_flag")));
        node.put("passenger_count", tripNode.get("passenger_count").asInt());
        node.put("trip_time_in_secs", tripNode.get("trip_time_in_secs").asLong());
        node.put("trip_distance", tripNode.get("trip_distance").asDouble(0));

        node.put("pickup_datetime", fareNode.get("pickup_datetime").asText());
        node.put("dropoff_datetime", tripNode.get("dropoff_datetime").asText());

        ArrayNode pickupNode = node.putArray("pickup_location");
        pickupNode.add(tripNode.get("pickup_location").get(0));
        pickupNode.add(tripNode.get("pickup_location").get(1));

        ArrayNode dropOffNode = node.putArray("dropoff_location");
        dropOffNode.add(tripNode.get("dropoff_location").get(0));
        dropOffNode.add(tripNode.get("dropoff_location").get(1));

        return node;
    }

    private static String ifNotEmpty(JsonNode node) {
        if (node == null) {
            return "";
        } else {
            return node.asText();
        }
    }
}
