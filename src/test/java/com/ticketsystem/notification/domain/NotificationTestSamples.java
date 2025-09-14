package com.ticketsystem.notification.domain;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class NotificationTestSamples {

    private static final Random random = new Random();
    private static final AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    public static Notification getNotificationSample1() {
        return new Notification()
            .id(1L)
            .recipientId(UUID.fromString("23d8dc04-a48b-45d9-a01d-4b728f0ad4aa"))
            .templateType("templateType1")
            .templateLanguage("templateLanguage1")
            .channel("channel1")
            .metadata("metadata1")
            .status("status1")
            .bookingId(UUID.fromString("23d8dc04-a48b-45d9-a01d-4b728f0ad4aa"));
    }

    public static Notification getNotificationSample2() {
        return new Notification()
            .id(2L)
            .recipientId(UUID.fromString("ad79f240-3727-46c3-b89f-2cf6ebd74367"))
            .templateType("templateType2")
            .templateLanguage("templateLanguage2")
            .channel("channel2")
            .metadata("metadata2")
            .status("status2")
            .bookingId(UUID.fromString("ad79f240-3727-46c3-b89f-2cf6ebd74367"));
    }

    public static Notification getNotificationRandomSampleGenerator() {
        return new Notification()
            .id(longCount.incrementAndGet())
            .recipientId(UUID.randomUUID())
            .templateType(UUID.randomUUID().toString())
            .templateLanguage(UUID.randomUUID().toString())
            .channel(UUID.randomUUID().toString())
            .metadata(UUID.randomUUID().toString())
            .status(UUID.randomUUID().toString())
            .bookingId(UUID.randomUUID());
    }
}
