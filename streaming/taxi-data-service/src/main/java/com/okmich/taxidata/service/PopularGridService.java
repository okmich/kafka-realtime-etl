package com.okmich.taxidata.service;

import com.fasterxml.jackson.databind.JsonNode;
import static com.okmich.taxidata.service.Configuration.JSON_SERDE;
import static com.okmich.taxidata.service.Configuration.KAFKA_BROKER;
import com.okmich.taxidata.service.util.GeoUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class PopularGridService implements Configuration {

    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "PopularGridService");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //to do
        KStream<String, JsonNode> tripStream = builder.stream(REFINED_TRIP_TOPIC, Consumed.with(Serdes.String(), JSON_SERDE));

        //map it to pair (Integer, 1)
        KStream<Integer, Integer> nycGridStream = tripStream.map((String key, JsonNode value) -> {
            float lon = (float) value.get("pickup_location").get(0).asDouble();
            float lat = (float) value.get("pickup_location").get(1).asDouble();

            Integer gridId = -1;
            if (GeoUtils.isInNYC(lon, lat)) {
                gridId = GeoUtils.mapToGridCell(lon, lat);
            }

            return new KeyValue(gridId, 1);
        }).filter((Object k, Object v) -> ((Integer) k > -1));

        //get grid count in a sliding window
        KTable<Windowed<Integer>, Integer> gridCountTable
                = nycGridStream
                        .groupByKey().windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5)).advanceBy(TimeUnit.MINUTES.toMillis(1)))
                        .reduce((Integer value1, Integer value2) -> {
                            return value1 + value2;
                        });

        //write out to a stream
        gridCountTable.toStream()
                .map((Windowed<Integer> key, Integer value) -> {

                    System.out.println("From window " + key.toString() + ", -> key is " + key.key() + " and value is " + value);
                    return new KeyValue(key.key(), value);
                })
                .to(TRENDING_PICKUP_GRID_TOPIC);

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
