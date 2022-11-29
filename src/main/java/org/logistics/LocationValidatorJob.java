/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.logistics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.logistics.asyncLogicProcessor.AsyncSourceDataProcessor;
import org.logistics.models.FlinkJobProperties;
import org.logistics.models.LocationData;
import org.logistics.deserializers.LocationDataDeserializationSchema;
import org.logistics.models.PropertyConfig;
import org.logistics.serializers.LocationDataKeySerializationSchema;
import org.logistics.serializers.LocationDataValueSerializationSchema;
import org.logistics.util.LocationSinkTopicSelector;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class LocationValidatorJob {

    private final PropertyConfig propertyConfig = FlinkJobProperties.getInstance().getConfig();

    public static void main(String[] args) throws Exception {
        LocationValidatorJob locationValidatorJob = new LocationValidatorJob();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        locationValidatorJob.setEnv(env);

        KafkaSource<LocationData> kafkaSource = locationValidatorJob.configureAndGetKafkaSource();


        KafkaSink<LocationData> kafkaSink = locationValidatorJob.configureAndGetKafkaSink();

        DataStream<LocationData> inputMessagesStream = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(200)), "Kafka Source");

        inputMessagesStream
                .keyBy((LocationData::getDeviceId))
                .window(TumblingEventTimeWindows.of(Time.seconds(20)));

        DataStream<LocationData> resultStream = AsyncDataStream.unorderedWait(inputMessagesStream, new AsyncSourceDataProcessor(), 5000, TimeUnit.MILLISECONDS, 100);
        resultStream.sinkTo(kafkaSink);

        env.execute("Location Validator Flink Job");
    }

    public void setEnv(StreamExecutionEnvironment streamExecutionEnvironment) {
        streamExecutionEnvironment.setParallelism(5);
        streamExecutionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(propertyConfig.getFlink().getRestartAttempts(), propertyConfig.getFlink().getRestartDelay()));
    }

    public KafkaSource<LocationData> configureAndGetKafkaSource() {
        return KafkaSource
                .<LocationData>builder()
                .setBootstrapServers(propertyConfig.getKafka().getServerAddress())
                .setTopics(propertyConfig.getKafka().getSourceTopic())
                .setGroupId(propertyConfig.getKafka().getSourceGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new LocationDataDeserializationSchema())
                .build();
    }

    public KafkaSink<LocationData> configureAndGetKafkaSink() {
        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", propertyConfig.getKafka().getSinkTransactionTimeout());

        return KafkaSink.<LocationData>builder()
                .setBootstrapServers(propertyConfig.getKafka().getServerAddress())
                .setKafkaProducerConfig(properties)
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopicSelector(new LocationSinkTopicSelector())
                        .setValueSerializationSchema(new LocationDataValueSerializationSchema())
                        .setKeySerializationSchema(new LocationDataKeySerializationSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
