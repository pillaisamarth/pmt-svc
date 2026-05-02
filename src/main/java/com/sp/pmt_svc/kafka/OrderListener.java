package com.sp.pmt_svc.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class OrderListener {
    private final Map<String, Boolean> processedOrderIds = new ConcurrentHashMap<>();

    @KafkaListener(topics = "order-placed", errorHandler = "customErrorHandler", concurrency = "3")
    public void listenOrderPlaced(String message){
        log.info("Received OrderPlaced event: {}", message);

        if(Math.random() < 0.3){
            log.error("Simulated failure - throwing exception");
            throw new RuntimeException("Payment processing failed");
        }

        JsonNode node = new ObjectMapper().readTree(message);
        String orderId = String.valueOf(node.get("orderId"));

        Boolean processed = processedOrderIds.putIfAbsent(orderId, true);
        if(processed != null){
            log.info("Skipping duplicate orderId: {}", orderId);
            return;
        }
        log.info("Successfully processed order");
    }
}
