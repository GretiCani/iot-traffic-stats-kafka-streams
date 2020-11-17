package com.producer;

import com.java.avro.IotData;
import com.producer.util.PropertyReader;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.*;

public class DataProducer {

    private static final Logger logger = Logger.getLogger(DataProducer.class);

    public static void main(String[] args) throws Exception{
        Properties prop = PropertyReader.readPropertyFile();
        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,prop.getProperty("com.iot.app.kafka.brokerlist"));
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,prop.getProperty("com.iot.schema.registry"));
        Producer<String, IotData> producer = new KafkaProducer<String, IotData>(kafkaProps);
        generatePayload(producer);


    }

    public static void generatePayload( Producer<String, IotData> producer) throws InterruptedException {

        List<IotData> eventList;
        List<String> portals = Arrays.asList("P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9");
        List<String> vehicleTypeList = Arrays.asList(new String[]{"Large Truck", "Small Truck", "Private Car", "Bus", "Taxi"});
        List<String> routeList = Arrays.asList(new String[]{"Route-37", "Route-43", "Route-82"});
        Random random = new Random();
        Long index=10L;
        Calendar calendar = Calendar.getInstance();

        while (index<110){
            eventList = new ArrayList<>();
            for(int i=0; i<100;i++) {
              String vehicleId = UUID.randomUUID().toString();
              String vehicleType = vehicleTypeList.get(random.nextInt(5));
              String plate = generatePlate();
              String routeID = routeList.get(random.nextInt(3));
              long timestamp =  System.currentTimeMillis();
              logger.info(timestamp);
              boolean isAdr = index % 10 == 0;
              double speed = random.nextInt(100 - 20) + 20;

                  String portal = portals.get(random.nextInt(9));
                  String coords = getCoordinates(routeID);
                  String latitude = coords.substring(0, coords.indexOf(","));
                  String longitude = coords.substring(coords.indexOf(",") + 1);
                  IotData data = IotData.newBuilder()
                          .setVehicleId(vehicleId).setVehicleType(vehicleType)
                          .setPlate(plate).setRouteId(routeID).setLatitude(latitude)
                          .setLongitude(longitude).setTimestamp(timestamp)
                          .setSpeed(speed).setAdr(isAdr).setPortal(portal).build();
                  eventList.add(data);


              index++;
          }
            Collections.shuffle(eventList);
            for (IotData event :eventList){
                ProducerRecord<String,IotData> record = new ProducerRecord<String,IotData>("traffic-topic",event);
                producer.send(record, (recordMetadata, e) -> {
                    if(e==null)
                        logger.info(recordMetadata);
                    else  e.printStackTrace();

                });
                Thread.sleep(random.nextInt(3000 - 1000) + 1000);//random delay of 1 to 3 seconds
            }


        }

    }

    public static String generatePlate() {
        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        StringBuilder sb = new StringBuilder(5);

        for (int i = 0; i < 5; i++) {
            int index = (int) (alphaNumericString.length() * Math.random());
            sb.append(alphaNumericString.charAt(index));
        }
        return sb.toString();
    }

    private static String  getCoordinates(String routeId) {
        Random rand = new Random();
        int latPrefix = 0;
        int longPrefix = -0;
        if (routeId.equals("Route-37")) {
            latPrefix = 33;
            longPrefix = -96;
        } else if (routeId.equals("Route-82")) {
            latPrefix = 34;
            longPrefix = -97;
        } else if (routeId.equals("Route-43")) {
            latPrefix = 35;
            longPrefix = -98;
        }
        Float lati = latPrefix + rand.nextFloat();
        Float longi = longPrefix + rand.nextFloat();
        return lati + "," + longi;
    }


}




