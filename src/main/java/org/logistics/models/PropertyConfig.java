package org.logistics.models;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class PropertyConfig implements Serializable {

    private FlinkConfig flink;
    private JobConfig job;
    private KafkaConfig kafka;
    private RedisConfig redis;
}
