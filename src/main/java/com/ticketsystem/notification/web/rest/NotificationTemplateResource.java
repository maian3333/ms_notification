package com.ticketsystem.notification.web.rest;

import com.ticketsystem.notification.repository.NotificationTemplateRepository;
import com.ticketsystem.notification.service.NotificationTemplateQueryService;
import com.ticketsystem.notification.service.NotificationTemplateService;
import com.ticketsystem.notification.service.criteria.NotificationTemplateCriteria;
import com.ticketsystem.notification.service.dto.NotificationTemplateDTO;
import com.ticketsystem.notification.web.rest.errors.BadRequestAlertException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tech.jhipster.web.util.HeaderUtil;
import tech.jhipster.web.util.ResponseUtil;

/**
 * REST controller for managing {@link com.ticketsystem.notification.domain.NotificationTemplate}.
 */
@RestController
@RequestMapping("/api/notification-templates")
public class NotificationTemplateResource {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationTemplateResource.class);

    private static final String ENTITY_NAME = "msNotificationNotificationTemplate";

    @Value("${jhipster.clientApp.name}")
    private String applicationName;

    private final NotificationTemplateService notificationTemplateService;

    private final NotificationTemplateRepository notificationTemplateRepository;

    private final NotificationTemplateQueryService notificationTemplateQueryService;

    public NotificationTemplateResource(
        NotificationTemplateService notificationTemplateService,
        NotificationTemplateRepository notificationTemplateRepository,
        NotificationTemplateQueryService notificationTemplateQueryService
    ) {
        this.notificationTemplateService = notificationTemplateService;
        this.notificationTemplateRepository = notificationTemplateRepository;
        this.notificationTemplateQueryService = notificationTemplateQueryService;
    }

    /**
     * {@code POST  /notification-templates} : Create a new notificationTemplate.
     *
     * @param notificationTemplateDTO the notificationTemplateDTO to create.
     * @return the {@link ResponseEntity} with status {@code 201 (Created)} and with body the new notificationTemplateDTO, or with status {@code 400 (Bad Request)} if the notificationTemplate has already an ID.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PostMapping("")
    public ResponseEntity<NotificationTemplateDTO> createNotificationTemplate(
        @Valid @RequestBody NotificationTemplateDTO notificationTemplateDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to save NotificationTemplate : {}", notificationTemplateDTO);
        if (notificationTemplateDTO.getId() != null) {
            throw new BadRequestAlertException("A new notificationTemplate cannot already have an ID", ENTITY_NAME, "idexists");
        }
        notificationTemplateDTO = notificationTemplateService.save(notificationTemplateDTO);
        return ResponseEntity.created(new URI("/api/notification-templates/" + notificationTemplateDTO.getId()))
            .headers(HeaderUtil.createEntityCreationAlert(applicationName, true, ENTITY_NAME, notificationTemplateDTO.getId().toString()))
            .body(notificationTemplateDTO);
    }

    /**
     * {@code PUT  /notification-templates/:id} : Updates an existing notificationTemplate.
     *
     * @param id the id of the notificationTemplateDTO to save.
     * @param notificationTemplateDTO the notificationTemplateDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated notificationTemplateDTO,
     * or with status {@code 400 (Bad Request)} if the notificationTemplateDTO is not valid,
     * or with status {@code 500 (Internal Server Error)} if the notificationTemplateDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PutMapping("/{id}")
    public ResponseEntity<NotificationTemplateDTO> updateNotificationTemplate(
        @PathVariable(value = "id", required = false) final Long id,
        @Valid @RequestBody NotificationTemplateDTO notificationTemplateDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to update NotificationTemplate : {}, {}", id, notificationTemplateDTO);
        if (notificationTemplateDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, notificationTemplateDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!notificationTemplateRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        notificationTemplateDTO = notificationTemplateService.update(notificationTemplateDTO);
        return ResponseEntity.ok()
            .headers(HeaderUtil.createEntityUpdateAlert(applicationName, true, ENTITY_NAME, notificationTemplateDTO.getId().toString()))
            .body(notificationTemplateDTO);
    }

    /**
     * {@code PATCH  /notification-templates/:id} : Partial updates given fields of an existing notificationTemplate, field will ignore if it is null
     *
     * @param id the id of the notificationTemplateDTO to save.
     * @param notificationTemplateDTO the notificationTemplateDTO to update.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the updated notificationTemplateDTO,
     * or with status {@code 400 (Bad Request)} if the notificationTemplateDTO is not valid,
     * or with status {@code 404 (Not Found)} if the notificationTemplateDTO is not found,
     * or with status {@code 500 (Internal Server Error)} if the notificationTemplateDTO couldn't be updated.
     * @throws URISyntaxException if the Location URI syntax is incorrect.
     */
    @PatchMapping(value = "/{id}", consumes = { "application/json", "application/merge-patch+json" })
    public ResponseEntity<NotificationTemplateDTO> partialUpdateNotificationTemplate(
        @PathVariable(value = "id", required = false) final Long id,
        @NotNull @RequestBody NotificationTemplateDTO notificationTemplateDTO
    ) throws URISyntaxException {
        LOG.debug("REST request to partial update NotificationTemplate partially : {}, {}", id, notificationTemplateDTO);
        if (notificationTemplateDTO.getId() == null) {
            throw new BadRequestAlertException("Invalid id", ENTITY_NAME, "idnull");
        }
        if (!Objects.equals(id, notificationTemplateDTO.getId())) {
            throw new BadRequestAlertException("Invalid ID", ENTITY_NAME, "idinvalid");
        }

        if (!notificationTemplateRepository.existsById(id)) {
            throw new BadRequestAlertException("Entity not found", ENTITY_NAME, "idnotfound");
        }

        Optional<NotificationTemplateDTO> result = notificationTemplateService.partialUpdate(notificationTemplateDTO);

        return ResponseUtil.wrapOrNotFound(
            result,
            HeaderUtil.createEntityUpdateAlert(applicationName, true, ENTITY_NAME, notificationTemplateDTO.getId().toString())
        );
    }

    /**
     * {@code GET  /notification-templates} : get all the notificationTemplates.
     *
     * @param criteria the criteria which the requested entities should match.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and the list of notificationTemplates in body.
     */
    @GetMapping("")
    public ResponseEntity<List<NotificationTemplateDTO>> getAllNotificationTemplates(NotificationTemplateCriteria criteria) {
        LOG.debug("REST request to get NotificationTemplates by criteria: {}", criteria);

        List<NotificationTemplateDTO> entityList = notificationTemplateQueryService.findByCriteria(criteria);
        return ResponseEntity.ok().body(entityList);
    }

    /**
     * {@code GET  /notification-templates/count} : count all the notificationTemplates.
     *
     * @param criteria the criteria which the requested entities should match.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and the count in body.
     */
    @GetMapping("/count")
    public ResponseEntity<Long> countNotificationTemplates(NotificationTemplateCriteria criteria) {
        LOG.debug("REST request to count NotificationTemplates by criteria: {}", criteria);
        return ResponseEntity.ok().body(notificationTemplateQueryService.countByCriteria(criteria));
    }

    /**
     * {@code GET  /notification-templates/:id} : get the "id" notificationTemplate.
     *
     * @param id the id of the notificationTemplateDTO to retrieve.
     * @return the {@link ResponseEntity} with status {@code 200 (OK)} and with body the notificationTemplateDTO, or with status {@code 404 (Not Found)}.
     */
    @GetMapping("/{id}")
    public ResponseEntity<NotificationTemplateDTO> getNotificationTemplate(@PathVariable("id") Long id) {
        LOG.debug("REST request to get NotificationTemplate : {}", id);
        Optional<NotificationTemplateDTO> notificationTemplateDTO = notificationTemplateService.findOne(id);
        return ResponseUtil.wrapOrNotFound(notificationTemplateDTO);
    }

    /**
     * {@code DELETE  /notification-templates/:id} : delete the "id" notificationTemplate.
     *
     * @param id the id of the notificationTemplateDTO to delete.
     * @return the {@link ResponseEntity} with status {@code 204 (NO_CONTENT)}.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteNotificationTemplate(@PathVariable("id") Long id) {
        LOG.debug("REST request to delete NotificationTemplate : {}", id);
        notificationTemplateService.delete(id);
        return ResponseEntity.noContent()
            .headers(HeaderUtil.createEntityDeletionAlert(applicationName, true, ENTITY_NAME, id.toString()))
            .build();
    }
}
