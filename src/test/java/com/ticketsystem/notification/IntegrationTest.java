package com.ticketsystem.notification;

import com.ticketsystem.notification.config.AsyncSyncConfiguration;
import com.ticketsystem.notification.config.EmbeddedKafka;
import com.ticketsystem.notification.config.EmbeddedRedis;
import com.ticketsystem.notification.config.EmbeddedSQL;
import com.ticketsystem.notification.config.JacksonConfiguration;
import com.ticketsystem.notification.config.TestSecurityConfiguration;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Base composite annotation for integration tests.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(
    classes = { MsNotificationApp.class, JacksonConfiguration.class, AsyncSyncConfiguration.class, TestSecurityConfiguration.class }
)
@EmbeddedRedis
@EmbeddedSQL
@EmbeddedKafka
public @interface IntegrationTest {
}
