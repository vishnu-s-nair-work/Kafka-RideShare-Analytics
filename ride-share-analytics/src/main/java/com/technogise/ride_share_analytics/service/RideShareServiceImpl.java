package com.technogise.ride_share_analytics.service;

import com.technogise.avro.RidesEvent;
import com.technogise.ride_share_analytics.model.RideEvent;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RideShareServiceImpl implements RideShareService {

  private final KafkaTemplate<String, RidesEvent> kafkaTemplate;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private ExecutorService executor;

  private static final List<String> CITIES =
      List.of("Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai");

  public RideShareServiceImpl(KafkaTemplate<String, RidesEvent> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @Override
  public void startRideShareEventProduction(int producerCount) {

    if (!running.compareAndSet(false, true)) {
      log.info("Producer already running");
      return;
    }

    executor = Executors.newVirtualThreadPerTaskExecutor();

    for (int i = 0; i < producerCount; i++) {
      executor.submit(this::runProducerLoop);
    }
    log.info("Ride event producer started");
  }

  @Override
  public void stopRideShareEventProduction() {
    running.set(false);

    if (executor != null) {
      executor.shutdownNow();
    }

    log.info("Ride event producer stopped");
  }

  private void runProducerLoop() {
    try {
      while (running.get()) {
        produceOneRideLifecycle();
      }
    } catch (Exception e) {
      log.error("Producer loop crashed", e);
    }
  }

  private void produceOneRideLifecycle() {
    UUID rideId = UUID.randomUUID();
    String city = randomCity();

    for (RideEvent type : RideEvent.values()) {
      if (!running.get()) {
        return;
      }

      RidesEvent event =
          new RidesEvent(rideId.toString(), city, type.toString(), Instant.now().toString());

      kafkaTemplate.send("ride-events", rideId.toString(), event);

      log.info("Producing rideId={}, city={}, event={}", rideId, city, type);

      sleep(3); // delay between lifecycle events
    }

    sleep(2); // gap before next ride starts
  }

  private String randomCity() {
    return CITIES.get(ThreadLocalRandom.current().nextInt(CITIES.size()));
  }

  private void sleep(int seconds) {
    try {
      Thread.sleep(seconds * 1000L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
