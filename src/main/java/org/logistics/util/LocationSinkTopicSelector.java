package org.logistics.util;

import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.logistics.models.FlinkJobProperties;
import org.logistics.models.LocationData;
import org.logistics.models.PropertyConfig;

public class LocationSinkTopicSelector implements TopicSelector<LocationData> {

    private final PropertyConfig propertyConfig = FlinkJobProperties.getInstance().getConfig();

    @Override
    public String apply(LocationData locationData) {
        if(locationData.getEventType().equals("valid_location_data"))
            return propertyConfig.getKafka().getSinkTopicValid();
        return propertyConfig.getKafka().getSinkTopicInvalid();
    }
}
