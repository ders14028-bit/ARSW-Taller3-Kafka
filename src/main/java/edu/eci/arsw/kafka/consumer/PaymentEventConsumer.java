package edu.eci.arsw.kafka.consumer;

import java.time.Instant;
import java.util.UUID;

import edu.eci.arsw.kafka.dto.OrderCreatedEvent;
import edu.eci.arsw.kafka.dto.PaymentProcessedEvent;
import edu.eci.arsw.kafka.producer.PaymentEventProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentEventConsumer {

    private final PaymentEventProducer paymentEventProducer;

    public PaymentEventConsumer(PaymentEventProducer paymentEventProducer) {
        this.paymentEventProducer = paymentEventProducer;
    }

    @KafkaListener(topics = "orders", groupId = "payment-service", containerFactory = "orderCreatedEventKafkaListenerContainerFactory")
    public void consume(OrderCreatedEvent event) {
        boolean approved = event.getTotal() <= 250000;
        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
                "EVT-" + UUID.randomUUID(),
                event.getCorrelationId(),
                event.getOrderId(),
                event.getCustomerId(),
                event.getTotal(),
                approved ? "APPROVED" : "REJECTED",
                Instant.now());
        paymentEventProducer.publish(paymentEvent);
    }
}