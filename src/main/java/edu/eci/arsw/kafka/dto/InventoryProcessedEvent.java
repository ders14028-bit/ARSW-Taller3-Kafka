package edu.eci.arsw.kafka.dto;

import java.time.Instant;

public class InventoryProcessedEvent {

    private String eventId;
    private String correlationId;
    private String orderId;
    private String customerId;
    private String status;
    private Instant occurredAt;

    public InventoryProcessedEvent() {
    }

    public InventoryProcessedEvent(String eventId, String correlationId, String orderId, String customerId,
                                   String status, Instant occurredAt) {
        this.eventId = eventId;
        this.correlationId = correlationId;
        this.orderId = orderId;
        this.customerId = customerId;
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