package edu.eci.arsw.kafka.dto;

import java.time.Instant;

public class OrderCreatedEvent {

    private String eventId;
    private String correlationId;
    private String orderId;
    private String customerId;
    private Double total;
    private String status;
    private Instant occurredAt;

    public OrderCreatedEvent() {
    }

    public OrderCreatedEvent(String eventId, String correlationId, String orderId, String customerId, Double total,
                            String status, Instant occurredAt) {
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.total = total;
        this.status = status;
        this.occurredAt = occurredAt;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Double getTotal() {
        return total;
    }

    public void setTotal(Double total) {
        this.total = total;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }
}