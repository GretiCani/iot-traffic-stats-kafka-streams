package com.streams;

import com.java.avro.IotData;
import com.java.avro.TrafficStatistic;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class StreamApplication {

    public static void main(String[] args) {

        StreamApplication app = new StreamApplication();
        KafkaStreams stream = app.createTopology(app.getKafkaStreamsConfig());
        stream.cleanUp();
        stream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

    }

    private Properties getKafkaStreamsConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://172.17.0.1:8081");


        return config;
    }

    private KafkaStreams createTopology(Properties config){
        SpecificAvroSerde<IotData> iotDataSpecificAvroSerde = new SpecificAvroSerde<>();
        iotDataSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://172.17.0.1:8081"), false);

        Serdes.StringSerde stringSerde = new Serdes.StringSerde();
        Serdes.LongSerde longSerde = new Serdes.LongSerde();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,IotData> trafficStream = builder.stream("traffic-topic",Consumed.with(stringSerde,iotDataSpecificAvroSerde))
                .selectKey((key,data)-> data.getPortal());

        SpecificAvroSerde<TrafficStatistic> trafficStatisticSpecificAvroSerde = new SpecificAvroSerde<>();
        trafficStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://172.17.0.1:8081"), false);


        KTable<String,TrafficStatistic> trafficStatisticKTable = trafficStream.groupByKey()
                .aggregate(
                        this::emptyTrafficStats,
                        this::trafficStatisticAggregator,
                        Materialized.<String,TrafficStatistic, KeyValueStore< Bytes,byte[]>>as("long-term-stats")
                        .withValueSerde(trafficStatisticSpecificAvroSerde)
                );

        trafficStatisticKTable.toStream()
                .to("long-term-stats",Produced.with(stringSerde,trafficStatisticSpecificAvroSerde));

        Duration windowSizeDuration = Duration.ofMinutes(5);
        Duration advanceDuration = Duration.ofSeconds(1);
        long windowSizeMs = windowSizeDuration.toMillis();
        long advanceMs = advanceDuration.toMillis();

        TimeWindows timeWindows =TimeWindows.of(windowSizeDuration).advanceBy(advanceDuration);


        KTable<Windowed<String>,TrafficStatistic> windowedTrafficStatisticKTable = trafficStream
                .filter((k,v)->!isTrafficDataExpired(v,windowSizeMs))
                .groupByKey()
                .windowedBy(timeWindows)
                .aggregate(
                        this::emptyTrafficStats,
                        this::trafficStatisticAggregator,
                        Materialized.<String,TrafficStatistic, WindowStore<Bytes,byte[]>>as("recent-stats1")
                        .withValueSerde(trafficStatisticSpecificAvroSerde)
                );

        KStream<String,TrafficStatistic> recentStats = windowedTrafficStatisticKTable
        .toStream()
                .filter((window,traficStat)->keepCurrentWindow(window,advanceMs))
        .peek((key,value)-> System.err.println(value.toString()))
        .selectKey((k,v)->k.key());

        recentStats.to("recent-stats1",Produced.with(stringSerde,trafficStatisticSpecificAvroSerde));



        return new KafkaStreams(builder.build(),config);
    }


    private TrafficStatistic emptyTrafficStats(){
        return TrafficStatistic.newBuilder().build();
    }

    private TrafficStatistic trafficStatisticAggregator(String plate, IotData data, TrafficStatistic currentStats){

        Integer increment = 1;
        TrafficStatistic.Builder trafficStatisticBuilder = TrafficStatistic.newBuilder(currentStats);

        trafficStatisticBuilder.setAllTraffic(trafficStatisticBuilder.getAllTraffic()+increment);

        if(data.getAdr()) trafficStatisticBuilder.setAdr(trafficStatisticBuilder.getAdr()+increment);
        else trafficStatisticBuilder.setNotAdr(trafficStatisticBuilder.getNotAdr()+increment);

        switch (data.getVehicleType()){
            case "Large Truck":
                trafficStatisticBuilder.setLargeTruck(trafficStatisticBuilder.getLargeTruck()+increment);
                break;
            case "Small Truck":
                trafficStatisticBuilder.setSmallTruck(trafficStatisticBuilder.getSmallTruck()+increment);
                break;
            case "Private Car":
                trafficStatisticBuilder.setPrivateCar(trafficStatisticBuilder.getPrivateCar()+increment);
                break;
            case "Bus":
                trafficStatisticBuilder.setBus(trafficStatisticBuilder.getBus()+increment);
                break;
            case "Taxi":
                trafficStatisticBuilder.setTaxi(trafficStatisticBuilder.getTaxi()+increment);
                break;
        }

        switch (data.getRouteId()){
            case "Route-37":
                trafficStatisticBuilder.setRoute37(trafficStatisticBuilder.getRoute37()+increment);
                break;
            case "Route-43":
                trafficStatisticBuilder.setRoute43(trafficStatisticBuilder.getRoute43()+increment);
                break;
            case "Route-82":
                trafficStatisticBuilder.setRoute82(trafficStatisticBuilder.getRoute82()+increment);
                break;
        }

        return trafficStatisticBuilder.build();
    }

    private Boolean isTrafficDataExpired(IotData data, Long maxTime){
        return (data.getTimestamp()+maxTime) < System.currentTimeMillis();
    }

    private boolean keepCurrentWindow(Windowed<String> window,long advanceMs){
        long now = System.currentTimeMillis();
        return  window.window().end() > now &&
                window.window().end() < now + advanceMs;
    }



}
