package com.ticketsystem.notification.service.mapper;

import com.ticketsystem.notification.domain.Notification;
import com.ticketsystem.notification.domain.NotificationTemplate;
import com.ticketsystem.notification.service.dto.NotificationDTO;
import com.ticketsystem.notification.service.dto.NotificationTemplateDTO;
import org.mapstruct.*;

/**
 * Mapper for the entity {@link Notification} and its DTO {@link NotificationDTO}.
 */
@Mapper(componentModel = "spring")
public interface NotificationMapper extends EntityMapper<NotificationDTO, Notification> {
    @Mapping(target = "template", source = "template", qualifiedByName = "notificationTemplateId")
    NotificationDTO toDto(Notification s);

    @Named("notificationTemplateId")
    @BeanMapping(ignoreByDefault = true)
    @Mapping(target = "id", source = "id")
    NotificationTemplateDTO toDtoNotificationTemplateId(NotificationTemplate notificationTemplate);
}
