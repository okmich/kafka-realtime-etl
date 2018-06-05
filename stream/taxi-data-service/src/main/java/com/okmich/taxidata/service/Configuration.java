package com.okmich.taxidata.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public interface Configuration {

    DateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    DateFormat DATE_FORMATTER_2 = new SimpleDateFormat("yyyyMMddHHmm");

    String KAFKA_BROKER = "datadev-lab:9092";

    /**
     * RAW_TRIP_TOPIC
     */
    String RAW_TRIP_TOPIC = "trips-raw-stream-topic";
    /**
     * RAW_FARE_TOPIC
     */
    String RAW_FARE_TOPIC = "fares-raw-stream-topic";
    /**
     * REFINED_TRIP_TOPIC
     */
    String REFINED_TRIP_TOPIC = "trips-refined-stream-topic";
    /**
     * REFINED_FARE_TOPIC
     */
    String REFINED_FARE_TOPIC = "fares-refined-stream-topic";
    /**
     * MERGED_TRIP_FARES_TOPIC
     */
    String MERGED_TRIP_FARES_TOPIC = "trips-fares-join-topic";
    /**
     * TRENDING_PICKUP_GRID_TOPIC
     */
    String TRENDING_PICKUP_GRID_TOPIC = "trending-pickup-grid-topic";
    /**
     * driver-payment-statement-topic
     */
    String DRIVER_PAYMENT_STATEMENT_TOPIC = "driver-payment-statement-topic";
    /**
     * DRIVER_PAYMENT_SUMMARY_TOPIC
     */
    String DRIVER_PAYMENT_SUMMARY_TOPIC = "driver-payment-summary-topic";

    /**
     *
     */
    final Serde<JsonNode> JSON_SERDE = Serdes.serdeFrom(new JsonSerializer(),
            new JsonDeserializer());

    /**
     *
     * @param payload
     * @param pickupIdx
     * @return
     */
    public static String getKey(String payload, final int pickupIdx) {
        String yyyyMMddHHmm;
        String[] fields = payload.split(",");

        try {

            yyyyMMddHHmm = DATE_FORMATTER_2.format(DATE_FORMATTER.parse(fields[pickupIdx]));
        } catch (ParseException ex) {
            yyyyMMddHHmm = "-";
        }

        return fields[0] + fields[1] + yyyyMMddHHmm;
    }
}
