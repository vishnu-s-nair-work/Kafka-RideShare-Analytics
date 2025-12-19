package com.technogise.ride_share_analytics.service;

import org.springframework.stereotype.Service;

@Service
public interface RideShareService {
    void startRideShareEventProduction();
    void stopRideShareEventProduction();
}
