package com.ticketsystem.notification.service;

import com.ticketsystem.notification.domain.*; // for static metamodels
import com.ticketsystem.notification.domain.NotificationTemplate;
import com.ticketsystem.notification.repository.NotificationTemplateRepository;
import com.ticketsystem.notification.service.criteria.NotificationTemplateCriteria;
import com.ticketsystem.notification.service.dto.NotificationTemplateDTO;
import com.ticketsystem.notification.service.mapper.NotificationTemplateMapper;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tech.jhipster.service.QueryService;

/**
 * Service for executing complex queries for {@link NotificationTemplate} entities in the database.
 * The main input is a {@link NotificationTemplateCriteria} which gets converted to {@link Specification},
 * in a way that all the filters must apply.
 * It returns a {@link List} of {@link NotificationTemplateDTO} which fulfills the criteria.
 */
@Service
@Transactional(readOnly = true)
public class NotificationTemplateQueryService extends QueryService<NotificationTemplate> {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationTemplateQueryService.class);

    private final NotificationTemplateRepository notificationTemplateRepository;

    private final NotificationTemplateMapper notificationTemplateMapper;

    public NotificationTemplateQueryService(
        NotificationTemplateRepository notificationTemplateRepository,
        NotificationTemplateMapper notificationTemplateMapper
    ) {
        this.notificationTemplateRepository = notificationTemplateRepository;
        this.notificationTemplateMapper = notificationTemplateMapper;
    }

    /**
     * Return a {@link List} of {@link NotificationTemplateDTO} which matches the criteria from the database.
     * @param criteria The object which holds all the filters, which the entities should match.
     * @return the matching entities.
     */
    @Transactional(readOnly = true)
    public List<NotificationTemplateDTO> findByCriteria(NotificationTemplateCriteria criteria) {
        LOG.debug("find by criteria : {}", criteria);
        final Specification<NotificationTemplate> specification = createSpecification(criteria);
        return notificationTemplateMapper.toDto(notificationTemplateRepository.findAll(specification));
    }

    /**
     * Return the number of matching entities in the database.
     * @param criteria The object which holds all the filters, which the entities should match.
     * @return the number of matching entities.
     */
    @Transactional(readOnly = true)
    public long countByCriteria(NotificationTemplateCriteria criteria) {
        LOG.debug("count by criteria : {}", criteria);
        final Specification<NotificationTemplate> specification = createSpecification(criteria);
        return notificationTemplateRepository.count(specification);
    }

    /**
     * Function to convert {@link NotificationTemplateCriteria} to a {@link Specification}
     * @param criteria The object which holds all the filters, which the entities should match.
     * @return the matching {@link Specification} of the entity.
     */
    protected Specification<NotificationTemplate> createSpecification(NotificationTemplateCriteria criteria) {
        Specification<NotificationTemplate> specification = Specification.where(null);
        if (criteria != null) {
            // This has to be called first, because the distinct method returns null
            specification = Specification.allOf(
                Boolean.TRUE.equals(criteria.getDistinct()) ? distinct(criteria.getDistinct()) : null,
                buildRangeSpecification(criteria.getId(), NotificationTemplate_.id),
                buildStringSpecification(criteria.getType(), NotificationTemplate_.type),
                buildStringSpecification(criteria.getLanguage(), NotificationTemplate_.language),
                buildStringSpecification(criteria.getSubject(), NotificationTemplate_.subject),
                buildStringSpecification(criteria.getSmsTemplate(), NotificationTemplate_.smsTemplate),
                buildStringSpecification(criteria.getPushTemplate(), NotificationTemplate_.pushTemplate)
            );
        }
        return specification;
    }
}
