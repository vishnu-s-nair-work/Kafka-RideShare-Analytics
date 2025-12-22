package com.technogise.ride_share_analytics.controller;

import com.technogise.ride_share_analytics.service.RideShareService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/rideShareProducer")
@RequiredArgsConstructor
public class RideShareController {

  private final RideShareService rideShareService;

  @GetMapping("/start")
  public ResponseEntity<String> startRide(@RequestParam(defaultValue = "5") int producerCount) {
    rideShareService.startRideShareEventProduction(producerCount);
    return ResponseEntity.ok("Producer Started to Send Events");
  }

  @GetMapping("/stop")
  public ResponseEntity<String> stopRide() {
    rideShareService.stopRideShareEventProduction();
    return ResponseEntity.ok("Producer Stopped to Send Events");
  }
}
