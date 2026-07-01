package edu.eci.arsw.kafka.controller;

import java.time.Instant;
import java.util.UUID;

import edu.eci.arsw.kafka.dto.CreateOrderRequest;
import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.producer.OrderEventProducer;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderEventProducer producer;

    public OrderController(OrderEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderCreatedEvent createOrder(@Valid @RequestBody CreateOrderRequest request) {
        String orderId = "ORD-" + UUID.randomUUID();
        String eventId = "EVT-" + UUID.randomUUID();
        String correlationId = "CORR-" + UUID.randomUUID();

        OrderCreatedEvent event = new OrderCreatedEvent(
                eventId,
                correlationId,
                orderId,
                request.getCustomerId(),
                request.getTotal(),
                "CREATED",
                Instant.now());

        producer.publishOrderCreated(event);
        return event;
    }
}