package org.logistics.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class FlinkConfig implements Serializable {

    @JsonProperty("restart_attempts")
    private int restartAttempts;
    @JsonProperty("restart_delay")
    private long restartDelay;
}
