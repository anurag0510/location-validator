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
public class KafkaConfig implements Serializable {

    private String serverAddress;
    private String sinkTopicInvalid;
    private String sinkTopicValid;
    private String sinkTransactionTimeout;
    private String sourceGroupId;
    private String sourceTopic;
}
