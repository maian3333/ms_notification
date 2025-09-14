package com.ticketsystem.notification.service;

import com.ticketsystem.notification.service.dto.NotificationDTO;
import java.util.Optional;
import java.util.UUID;

/**
 * Service Interface for managing {@link com.ticketsystem.notification.domain.Notification}.
 */
public interface NotificationService {
    /**
     * Save a notification.
     *
     * @param notificationDTO the entity to save.
     * @return the persisted entity.
     */
    NotificationDTO save(NotificationDTO notificationDTO);

    /**
     * Updates a notification.
     *
     * @param notificationDTO the entity to update.
     * @return the persisted entity.
     */
    NotificationDTO update(NotificationDTO notificationDTO);

    /**
     * Partially updates a notification.
     *
     * @param notificationDTO the entity to update partially.
     * @return the persisted entity.
     */
    Optional<NotificationDTO> partialUpdate(NotificationDTO notificationDTO);

    /**
     * Get the "id" notification.
     *
     * @param id the id of the entity.
     * @return the entity.
     */
    Optional<NotificationDTO> findOne(UUID id);

    /**
     * Delete the "id" notification.
     *
     * @param id the id of the entity.
     */
    void delete(UUID id);
}
