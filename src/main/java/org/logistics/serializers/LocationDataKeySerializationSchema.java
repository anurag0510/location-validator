package org.logistics.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.logistics.models.LocationData;

@Slf4j
public class LocationDataKeySerializationSchema implements SerializationSchema<LocationData> {

    private final ObjectMapper objectMapper;

    public LocationDataKeySerializationSchema() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(LocationData locationDataStats) {
        try {
            objectMapper.registerModule(new JavaTimeModule());
            return objectMapper.writeValueAsString(locationDataStats.getDeviceId()).getBytes();
        } catch (JsonProcessingException e) {
            log.error("failed to parse json error : {}", e);
        }
        return new byte[0];
    }
}
