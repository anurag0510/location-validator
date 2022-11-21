package org.logistics.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.logistics.models.LocationData;

import java.io.IOException;

public class LocationDataDeserializationSchema implements DeserializationSchema<LocationData> {

    private final ObjectMapper objectMapper;

    public LocationDataDeserializationSchema() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public LocationData deserialize(byte[] bytes) throws IOException {
        objectMapper.registerModule(new JavaTimeModule());
        return objectMapper.readValue(bytes, LocationData.class);
    }

    @Override
    public boolean isEndOfStream(LocationData locationData) {
        return false;
    }

    @Override
    public TypeInformation<LocationData> getProducedType() {
        return TypeInformation.of(LocationData.class);
    }
}
