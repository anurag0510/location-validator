package org.logistics.asyncLogicProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.logistics.models.FlinkJobProperties;
import org.logistics.models.LocationData;
import org.logistics.models.PropertyConfig;
import org.redisson.Redisson;
import org.redisson.api.RFuture;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.Collections;

@Slf4j
public class AsyncSourceDataProcessor extends RichAsyncFunction<LocationData, LocationData> {

    private transient RedissonClient redissonClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PropertyConfig propertyConfig = FlinkJobProperties.getInstance().getConfig();
    private static long newEventsCount = 0;
    private static long totalEventsCount = 0;
    private static long totalValidEventsCount = 0;
    private static long totalInvalidEventsCount = 0;

    @Override
    public void open(Configuration parameters) {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(propertyConfig.getRedis().getServerAddress())
                .setPassword(propertyConfig.getRedis().getPassword());
        redissonClient = Redisson.create(config);
    }

    @Override
    public void close() {
        redissonClient.shutdown();
    }

    @Override
    public void asyncInvoke(LocationData locationData, ResultFuture<LocationData> resultFuture) {
        RMap<String, String> redissonMap = redissonClient.getMap(propertyConfig.getRedis().getReadWriteMap());
        final RFuture<String> future = redissonMap.getAsync(locationData.getDeviceId());

        future.whenComplete((res, exception) -> {
            try {
                if (res != null) {
                    totalEventsCount++;
                    LocationData oldValidLocationData = objectMapper.readValue(res, LocationData.class);
                    log.info("pre-saved redis location data for device : {}", oldValidLocationData);
                    log.info("newly received location for device : {}", locationData);
                    double calculatedDistance = calculateDistance(oldValidLocationData.getLat(), oldValidLocationData.getLng(), locationData.getLat(), locationData.getLng());
                    double calculatedHours = ((double) Math.abs(oldValidLocationData.getTimestamp() - locationData.getTimestamp())) / (60 * 60 * 1000);
                    double calculatedSpeed = calculatedDistance / calculatedHours;
                    if (calculatedSpeed > propertyConfig.getJob().getSpeedLimit()) {
                        totalInvalidEventsCount++;
                        log.info("calculated speed crossed validation check for device id {} : {}", locationData.getDeviceId(), calculatedSpeed);
                        locationData.setEventType("invalid_location_data");
                    } else {
                        totalValidEventsCount++;
                        log.info("calculated speed is under validation check for device id {} : {}", locationData.getDeviceId(), calculatedSpeed);
                        redissonMap.putAsync(locationData.getDeviceId(), locationData.toString());
                        locationData.setEventType("valid_location_data");
                    }
                } else {
                    totalEventsCount++;
                    newEventsCount++;
                    totalValidEventsCount++;
                    log.info("new device data received so gonna mark it as valid");
                    redissonMap.putAsync(locationData.getDeviceId(), locationData.toString());
                    locationData.setEventType("valid_location_data");
                }
                log.info("new events : {}, total events: {}, valid events : {}, invalid events: {}", newEventsCount, totalEventsCount, totalValidEventsCount, totalInvalidEventsCount);
                resultFuture.complete(Collections.singleton(locationData));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private double calculateDistance(double lat1, double lng1, double lat2, double lng2) {
        if ((lat1 == lat2) && (lng1 == lng2)) {
            return 0;
        } else {
            double theta = lng1 - lng2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                    * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            dist = dist * 1.609344;
            return (dist);
        }
    }
}
