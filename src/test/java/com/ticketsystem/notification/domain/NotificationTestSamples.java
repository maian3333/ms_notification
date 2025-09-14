package com.ticketsystem.notification.domain;

import java.util.UUID;

public class NotificationTestSamples {

    public static Notification getNotificationSample1() {
        return new Notification()
            .id(UUID.fromString("23d8dc04-a48b-45d9-a01d-4b728f0ad4aa"))
            .recipientId(UUID.fromString("23d8dc04-a48b-45d9-a01d-4b728f0ad4aa"))
            .type("type1")
            .title("title1")
            .relatedEntityType("relatedEntityType1")
            .relatedEntityId(UUID.fromString("23d8dc04-a48b-45d9-a01d-4b728f0ad4aa"));
    }

    public static Notification getNotificationSample2() {
        return new Notification()
            .id(UUID.fromString("ad79f240-3727-46c3-b89f-2cf6ebd74367"))
            .recipientId(UUID.fromString("ad79f240-3727-46c3-b89f-2cf6ebd74367"))
            .type("type2")
            .title("title2")
            .relatedEntityType("relatedEntityType2")
            .relatedEntityId(UUID.fromString("ad79f240-3727-46c3-b89f-2cf6ebd74367"));
    }

    public static Notification getNotificationRandomSampleGenerator() {
        return new Notification()
            .id(UUID.randomUUID())
            .recipientId(UUID.randomUUID())
            .type(UUID.randomUUID().toString())
            .title(UUID.randomUUID().toString())
            .relatedEntityType(UUID.randomUUID().toString())
            .relatedEntityId(UUID.randomUUID());
    }
}
