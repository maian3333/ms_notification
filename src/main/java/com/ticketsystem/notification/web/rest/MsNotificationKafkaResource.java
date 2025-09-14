package com.ticketsystem.notification.web.rest;

import com.ticketsystem.notification.broker.KafkaProducer;
import com.ticketsystem.notification.service.dto.NotificationDTO;
import com.ticketsystem.kafka.service.KafkaUtilityService;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/ms-notification-kafka")
public class MsNotificationKafkaResource {

    private static final Logger LOG = LoggerFactory.getLogger(MsNotificationKafkaResource.class);

    private final KafkaProducer kafkaProducer; // now only has get() + send()
    private final KafkaUtilityService kafkaUtilityService;

    public MsNotificationKafkaResource(
            KafkaProducer kafkaProducer,
            KafkaUtilityService kafkaUtilityService) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaUtilityService = kafkaUtilityService;
    }

    /**
     * Test endpoint to send a simple message
     */
    @PostMapping("/publish/simple")
    public ResponseEntity<Map<String, String>> publishSimple(@RequestParam("message") String message) {
        LOG.debug("REST request to send simple message: {} to Kafka via Supplier", message);

        try {
            String messageKey = kafkaProducer.send("notification.test", message);
            if (messageKey == null) {
                throw new IllegalStateException("Failed to queue event (null key returned)");
            }

            Map<String, String> response = new HashMap<>();
            response.put("message", "Message queued successfully");
            response.put("messageKey", messageKey);
            response.put("payload", message);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Error queueing simple message: {}", e.getMessage(), e);
            Map<String, String> error = new HashMap<>();
            error.put("error", "Failed to queue message: " + e.getMessage());
            return ResponseEntity.internalServerError().body(error);
        }
    }

    /**
     * Send a notification creation event
     */
    @PostMapping("/publish/notification-created")
    public ResponseEntity<Map<String, String>> publishNotificationCreated(@RequestBody NotificationDTO notificationDTO) {
        LOG.debug("REST request to queue notification created event for: {}", notificationDTO);

        try {
            String messageKey = kafkaProducer.send("notification.created", notificationDTO);
            if (messageKey == null) {
                throw new IllegalStateException("Failed to queue event (null key returned)");
            }

            Map<String, String> response = new HashMap<>();
            response.put("message", "Notification created event queued successfully");
            response.put("messageKey", messageKey);
            response.put("notificationId", String.valueOf(notificationDTO.getId()));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Error queueing notification created event: {}", e.getMessage(), e);
            Map<String, String> error = new HashMap<>();
            error.put("error", "Failed to queue notification created event: " + e.getMessage());
            return ResponseEntity.internalServerError().body(error);
        }
    }

    /**
     * Send a notification updated event
     */
    @PostMapping("/publish/notification-updated")
    public ResponseEntity<Map<String, String>> publishNotificationUpdated(@RequestBody NotificationDTO notificationDTO) {
        LOG.debug("REST request to queue notification updated event for: {}", notificationDTO);
        try {
            String messageKey = kafkaProducer.send("notification.updated", notificationDTO);
            if (messageKey == null) {
                throw new IllegalStateException("Failed to queue event (null key returned)");
            }

            Map<String, String> response = new HashMap<>();
            response.put("message", "Notification updated event queued successfully");
            response.put("messageKey", messageKey);
            response.put("notificationId", String.valueOf(notificationDTO.getId()));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Error queueing notification updated event: {}", e.getMessage(), e);
            Map<String, String> error = new HashMap<>();
            error.put("error", "Failed to queue notification updated event: " + e.getMessage());
            return ResponseEntity.internalServerError().body(error);
        }

    }

    /**
     * Send a notification deleted event
     */
    @DeleteMapping("/publish/notification-deleted/{id}")
    public ResponseEntity<Map<String, String>> publishNotificationDeleted(@PathVariable Long id) {
        LOG.debug("REST request to queue notification deleted event for ID: {}", id);

        try {
            String messageKey = kafkaProducer.send("notification.deleted", id);
            if (messageKey == null) {
                throw new IllegalStateException("Failed to queue event (null key returned)");
            }

            Map<String, String> response = new HashMap<>();
            response.put("message", "Notification deleted event queued successfully");
            response.put("messageKey", messageKey);
            response.put("notificationId", String.valueOf(id));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            LOG.error("Error queueing notification deleted event: {}", e.getMessage(), e);
            Map<String, String> error = new HashMap<>();
            error.put("error", "Failed to queue notification deleted event: " + e.getMessage());
            return ResponseEntity.internalServerError().body(error);
        }
    }

    /**
     * Get Kafka utility service status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("service", "ms_notification");
        status.put("kafkaUtilityEnabled", true);
        status.put("sseClientsCount", kafkaUtilityService.getEmitters().size());
        status.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(status);
    }
}
