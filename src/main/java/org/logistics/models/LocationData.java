package org.logistics.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class LocationData {

    @JsonProperty("device_id")
    private String deviceId;
    @JsonProperty("event_type")
    private String eventType;
    private Long timestamp;
    private Double lat;
    private Double lng;

    @Override
    public String toString() {
        return "{\"device_id\":\"" + deviceId +
                "\",\"event_type\":\"" + eventType +
                "\", \"timestamp\":" + timestamp +
                ", \"lat\":" + lat +
                ", \"lng\":" + lng +
                '}';
    }
}
