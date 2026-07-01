package edu.eci.arsw.kafka.consumer;

import java.time.Instant;
import java.util.UUID;

import edu.eci.arsw.kafka.dto.InventoryProcessedEvent;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.producer.InventoryEventProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer {

    private final InventoryEventProducer inventoryEventProducer;

    public OrderEventConsumer(InventoryEventProducer inventoryEventProducer) {
        this.inventoryEventProducer = inventoryEventProducer;
    }

    @KafkaListener(topics = "orders", groupId = "inventory-service", containerFactory = "inventoryKafkaListenerContainerFactory")
    public void consume(OrderCreatedEvent event) {
        boolean reserved = event.getTotal() <= 300000;
        InventoryProcessedEvent inventoryEvent = new InventoryProcessedEvent(
                "EVT-" + UUID.randomUUID(),
                event.getCorrelationId(),
                event.getOrderId(),
                event.getCustomerId(),
                reserved ? "RESERVED" : "REJECTED",
                Instant.now());
        inventoryEventProducer.publish(inventoryEvent);
    }
}