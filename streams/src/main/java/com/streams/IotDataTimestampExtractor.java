package com.streams;

import com.java.avro.IotData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class IotDataTimestampExtractor  implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long previousTimestamp) {
        long timestamp = -1;
        final IotData data = (IotData) consumerRecord.value();

        if(data!=null) timestamp = data.getTimestamp();
        if (timestamp<0){
            if (previousTimestamp>= 0) return previousTimestamp;
            else return System.currentTimeMillis();
        } else return timestamp;

    }
}
