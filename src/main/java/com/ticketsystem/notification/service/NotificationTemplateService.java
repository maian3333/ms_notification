package com.ticketsystem.notification.service;

import com.ticketsystem.notification.service.dto.NotificationTemplateDTO;
import java.util.Optional;

/**
 * Service Interface for managing {@link com.ticketsystem.notification.domain.NotificationTemplate}.
 */
public interface NotificationTemplateService {
    /**
     * Save a notificationTemplate.
     *
     * @param notificationTemplateDTO the entity to save.
     * @return the persisted entity.
     */
    NotificationTemplateDTO save(NotificationTemplateDTO notificationTemplateDTO);

    /**
     * Updates a notificationTemplate.
     *
     * @param notificationTemplateDTO the entity to update.
     * @return the persisted entity.
     */
    NotificationTemplateDTO update(NotificationTemplateDTO notificationTemplateDTO);

    /**
     * Partially updates a notificationTemplate.
     *
     * @param notificationTemplateDTO the entity to update partially.
     * @return the persisted entity.
     */
    Optional<NotificationTemplateDTO> partialUpdate(NotificationTemplateDTO notificationTemplateDTO);

    /**
     * Get the "id" notificationTemplate.
     *
     * @param id the id of the entity.
     * @return the entity.
     */
    Optional<NotificationTemplateDTO> findOne(Long id);

    /**
     * Delete the "id" notificationTemplate.
     *
     * @param id the id of the entity.
     */
    void delete(Long id);
}
