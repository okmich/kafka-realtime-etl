package com.okmich.taxidata.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import static com.okmich.taxidata.service.Configuration.DATE_FORMATTER;
import static com.okmich.taxidata.service.Configuration.JSON_SERDE;
import static com.okmich.taxidata.service.Configuration.KAFKA_BROKER;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class PeriodicDriverPaymentService implements Configuration {

    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "PeriodicDriverPaymentService");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        prop.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //to do
        KStream<String, JsonNode> faresStream = builder.stream(REFINED_FARE_TOPIC, Consumed.with(Serdes.String(), JSON_SERDE));
        //filter for credit card payment
        KStream<String, JsonNode> creditCardPaymntStream = faresStream.filter((String k, JsonNode v) -> {

            return isEligibleForPayment(v);
        });

        //processing the amount payable 
        KStream<String, JsonNode> processedPaymntStream = creditCardPaymntStream.map((String key, JsonNode value) -> {
            String hackLicense = value.get("hack_license").asText();

            return new KeyValue(hackLicense, processPayment(value));
        });
        //send the payment to a topic
        KStream<String, JsonNode> paymentStatmntStream
                = processedPaymntStream.through(DRIVER_PAYMENT_STATEMENT_TOPIC,
                        Produced.with(Serdes.String(), JSON_SERDE));

        //group by hack license with a tumbling window
        TimeWindowedKStream<String, JsonNode> timedPaymentSummaryStream = paymentStatmntStream
                .groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(2)));
        //reduce 
//        KTable<Windowed<String>, JsonNode> paymentSummaryTable = timedPaymentSummaryStream.
//                reduce((JsonNode node1, JsonNode node2) -> aggregatePaymentSummary(node1, node2));

        KTable<Windowed<String>, JsonNode> paymentSummaryTable = timedPaymentSummaryStream.
                aggregate(() -> {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();

                    node.put("hack_license", "");
                    node.put("total_amount", 0);
                    node.put("payable_amount", 0);
                    node.put("txn_date", DATE_FORMATTER.format(new Date()));

                    return node;
                }, (String k, JsonNode v, JsonNode va) -> aggregatePaymentSummary(v, va));

        //send the table to another topic
        paymentSummaryTable.toStream().
                map((Windowed<String> key, JsonNode value) -> new KeyValue(key.key(), value)).
                to(DRIVER_PAYMENT_SUMMARY_TOPIC, Produced.with(Serdes.String(), JSON_SERDE));

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

    public static boolean isEligibleForPayment(JsonNode value) {
        String paymntType = value.get("payment_type").asText();
        return paymntType != null && !paymntType.equalsIgnoreCase("CAS");
    }

    /**
     *
     * @param value
     * @return
     */
    public static JsonNode processPayment(JsonNode value) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        double tip = value.get("tip_amount").asDouble(0);
        double fareAmt = value.get("fare_amount").asDouble(0);

        double payable = tip + (0.9 * fareAmt);

        node.put("hack_license", value.get("hack_license").asText());
        node.put("pickup_datetime", value.get("pickup_datetime").asText());
        node.put("total_amount", value.get("total_amount").asDouble(0));
        node.put("payable_amount", payable);

        return node;
    }

    public static JsonNode aggregatePaymentSummary(JsonNode node1, JsonNode node2) {
        System.out.println(node1);
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        node.put("hack_license", node2.get("hack_license").asText());
        node.put("total_amount", node1.get("total_amount").asDouble(0) + node2.get("total_amount").asDouble(0));
        node.put("payable_amount", node1.get("payable_amount").asDouble(0) + node2.get("payable_amount").asDouble(0));
        node.put("txn_date", DATE_FORMATTER.format(new Date()));

        return node;
    }
}
