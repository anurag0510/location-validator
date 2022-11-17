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
public class RedisConfig implements Serializable {

    private String password;
    private String readWriteMap;
    private String serverAddress;
}
