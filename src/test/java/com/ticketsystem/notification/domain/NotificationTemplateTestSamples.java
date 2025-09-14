package com.ticketsystem.notification.domain;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class NotificationTemplateTestSamples {

    private static final Random random = new Random();
    private static final AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    public static NotificationTemplate getNotificationTemplateSample1() {
        return new NotificationTemplate()
            .id(1L)
            .type("type1")
            .language("language1")
            .subject("subject1")
            .smsTemplate("smsTemplate1")
            .pushTemplate("pushTemplate1");
    }

    public static NotificationTemplate getNotificationTemplateSample2() {
        return new NotificationTemplate()
            .id(2L)
            .type("type2")
            .language("language2")
            .subject("subject2")
            .smsTemplate("smsTemplate2")
            .pushTemplate("pushTemplate2");
    }

    public static NotificationTemplate getNotificationTemplateRandomSampleGenerator() {
        return new NotificationTemplate()
            .id(longCount.incrementAndGet())
            .type(UUID.randomUUID().toString())
            .language(UUID.randomUUID().toString())
            .subject(UUID.randomUUID().toString())
            .smsTemplate(UUID.randomUUID().toString())
            .pushTemplate(UUID.randomUUID().toString());
    }
}
