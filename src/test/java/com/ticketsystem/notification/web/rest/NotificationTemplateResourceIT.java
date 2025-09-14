package com.ticketsystem.notification.web.rest;

import static com.ticketsystem.notification.domain.NotificationTemplateAsserts.*;
import static com.ticketsystem.notification.web.rest.TestUtil.createUpdateProxyForBean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketsystem.notification.IntegrationTest;
import com.ticketsystem.notification.domain.NotificationTemplate;
import com.ticketsystem.notification.repository.NotificationTemplateRepository;
import com.ticketsystem.notification.service.dto.NotificationTemplateDTO;
import com.ticketsystem.notification.service.mapper.NotificationTemplateMapper;
import jakarta.persistence.EntityManager;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests for the {@link NotificationTemplateResource} REST controller.
 */
@IntegrationTest
@AutoConfigureMockMvc
@WithMockUser
class NotificationTemplateResourceIT {

    private static final String DEFAULT_TYPE = "AAAAAAAAAA";
    private static final String UPDATED_TYPE = "BBBBBBBBBB";

    private static final String DEFAULT_LANGUAGE = "AAAAAAAAAA";
    private static final String UPDATED_LANGUAGE = "BBBBBBBBBB";

    private static final String DEFAULT_SUBJECT = "AAAAAAAAAA";
    private static final String UPDATED_SUBJECT = "BBBBBBBBBB";

    private static final String DEFAULT_EMAIL_BODY = "AAAAAAAAAA";
    private static final String UPDATED_EMAIL_BODY = "BBBBBBBBBB";

    private static final String DEFAULT_SMS_TEMPLATE = "AAAAAAAAAA";
    private static final String UPDATED_SMS_TEMPLATE = "BBBBBBBBBB";

    private static final String DEFAULT_PUSH_TEMPLATE = "AAAAAAAAAA";
    private static final String UPDATED_PUSH_TEMPLATE = "BBBBBBBBBB";

    private static final String ENTITY_API_URL = "/api/notification-templates";
    private static final String ENTITY_API_URL_ID = ENTITY_API_URL + "/{id}";

    private static Random random = new Random();
    private static AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    @Autowired
    private ObjectMapper om;

    @Autowired
    private NotificationTemplateRepository notificationTemplateRepository;

    @Autowired
    private NotificationTemplateMapper notificationTemplateMapper;

    @Autowired
    private EntityManager em;

    @Autowired
    private MockMvc restNotificationTemplateMockMvc;

    private NotificationTemplate notificationTemplate;

    private NotificationTemplate insertedNotificationTemplate;

    /**
     * Create an entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static NotificationTemplate createEntity() {
        return new NotificationTemplate()
            .type(DEFAULT_TYPE)
            .language(DEFAULT_LANGUAGE)
            .subject(DEFAULT_SUBJECT)
            .emailBody(DEFAULT_EMAIL_BODY)
            .smsTemplate(DEFAULT_SMS_TEMPLATE)
            .pushTemplate(DEFAULT_PUSH_TEMPLATE);
    }

    /**
     * Create an updated entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static NotificationTemplate createUpdatedEntity() {
        return new NotificationTemplate()
            .type(UPDATED_TYPE)
            .language(UPDATED_LANGUAGE)
            .subject(UPDATED_SUBJECT)
            .emailBody(UPDATED_EMAIL_BODY)
            .smsTemplate(UPDATED_SMS_TEMPLATE)
            .pushTemplate(UPDATED_PUSH_TEMPLATE);
    }

    @BeforeEach
    void initTest() {
        notificationTemplate = createEntity();
    }

    @AfterEach
    void cleanup() {
        if (insertedNotificationTemplate != null) {
            notificationTemplateRepository.delete(insertedNotificationTemplate);
            insertedNotificationTemplate = null;
        }
    }

    @Test
    @Transactional
    void createNotificationTemplate() throws Exception {
        long databaseSizeBeforeCreate = getRepositoryCount();
        // Create the NotificationTemplate
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);
        var returnedNotificationTemplateDTO = om.readValue(
            restNotificationTemplateMockMvc
                .perform(
                    post(ENTITY_API_URL)
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsBytes(notificationTemplateDTO))
                )
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString(),
            NotificationTemplateDTO.class
        );

        // Validate the NotificationTemplate in the database
        assertIncrementedRepositoryCount(databaseSizeBeforeCreate);
        var returnedNotificationTemplate = notificationTemplateMapper.toEntity(returnedNotificationTemplateDTO);
        assertNotificationTemplateUpdatableFieldsEquals(
            returnedNotificationTemplate,
            getPersistedNotificationTemplate(returnedNotificationTemplate)
        );

        insertedNotificationTemplate = returnedNotificationTemplate;
    }

    @Test
    @Transactional
    void createNotificationTemplateWithExistingId() throws Exception {
        // Create the NotificationTemplate with an existing ID
        notificationTemplate.setId(1L);
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        long databaseSizeBeforeCreate = getRepositoryCount();

        // An entity with an existing ID cannot be created, so this API call must fail
        restNotificationTemplateMockMvc
            .perform(
                post(ENTITY_API_URL)
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the NotificationTemplate in the database
        assertSameRepositoryCount(databaseSizeBeforeCreate);
    }

    @Test
    @Transactional
    void checkTypeIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        notificationTemplate.setType(null);

        // Create the NotificationTemplate, which fails.
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        restNotificationTemplateMockMvc
            .perform(
                post(ENTITY_API_URL)
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void checkLanguageIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        notificationTemplate.setLanguage(null);

        // Create the NotificationTemplate, which fails.
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        restNotificationTemplateMockMvc
            .perform(
                post(ENTITY_API_URL)
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void checkSubjectIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        notificationTemplate.setSubject(null);

        // Create the NotificationTemplate, which fails.
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        restNotificationTemplateMockMvc
            .perform(
                post(ENTITY_API_URL)
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void getAllNotificationTemplates() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList
        restNotificationTemplateMockMvc
            .perform(get(ENTITY_API_URL + "?sort=id,desc"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.[*].id").value(hasItem(notificationTemplate.getId().intValue())))
            .andExpect(jsonPath("$.[*].type").value(hasItem(DEFAULT_TYPE)))
            .andExpect(jsonPath("$.[*].language").value(hasItem(DEFAULT_LANGUAGE)))
            .andExpect(jsonPath("$.[*].subject").value(hasItem(DEFAULT_SUBJECT)))
            .andExpect(jsonPath("$.[*].emailBody").value(hasItem(DEFAULT_EMAIL_BODY)))
            .andExpect(jsonPath("$.[*].smsTemplate").value(hasItem(DEFAULT_SMS_TEMPLATE)))
            .andExpect(jsonPath("$.[*].pushTemplate").value(hasItem(DEFAULT_PUSH_TEMPLATE)));
    }

    @Test
    @Transactional
    void getNotificationTemplate() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get the notificationTemplate
        restNotificationTemplateMockMvc
            .perform(get(ENTITY_API_URL_ID, notificationTemplate.getId()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.id").value(notificationTemplate.getId().intValue()))
            .andExpect(jsonPath("$.type").value(DEFAULT_TYPE))
            .andExpect(jsonPath("$.language").value(DEFAULT_LANGUAGE))
            .andExpect(jsonPath("$.subject").value(DEFAULT_SUBJECT))
            .andExpect(jsonPath("$.emailBody").value(DEFAULT_EMAIL_BODY))
            .andExpect(jsonPath("$.smsTemplate").value(DEFAULT_SMS_TEMPLATE))
            .andExpect(jsonPath("$.pushTemplate").value(DEFAULT_PUSH_TEMPLATE));
    }

    @Test
    @Transactional
    void getNotificationTemplatesByIdFiltering() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        Long id = notificationTemplate.getId();

        defaultNotificationTemplateFiltering("id.equals=" + id, "id.notEquals=" + id);

        defaultNotificationTemplateFiltering("id.greaterThanOrEqual=" + id, "id.greaterThan=" + id);

        defaultNotificationTemplateFiltering("id.lessThanOrEqual=" + id, "id.lessThan=" + id);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByTypeIsEqualToSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where type equals to
        defaultNotificationTemplateFiltering("type.equals=" + DEFAULT_TYPE, "type.equals=" + UPDATED_TYPE);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByTypeIsInShouldWork() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where type in
        defaultNotificationTemplateFiltering("type.in=" + DEFAULT_TYPE + "," + UPDATED_TYPE, "type.in=" + UPDATED_TYPE);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByTypeIsNullOrNotNull() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where type is not null
        defaultNotificationTemplateFiltering("type.specified=true", "type.specified=false");
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByTypeContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where type contains
        defaultNotificationTemplateFiltering("type.contains=" + DEFAULT_TYPE, "type.contains=" + UPDATED_TYPE);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByTypeNotContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where type does not contain
        defaultNotificationTemplateFiltering("type.doesNotContain=" + UPDATED_TYPE, "type.doesNotContain=" + DEFAULT_TYPE);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByLanguageIsEqualToSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where language equals to
        defaultNotificationTemplateFiltering("language.equals=" + DEFAULT_LANGUAGE, "language.equals=" + UPDATED_LANGUAGE);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByLanguageIsInShouldWork() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where language in
        defaultNotificationTemplateFiltering("language.in=" + DEFAULT_LANGUAGE + "," + UPDATED_LANGUAGE, "language.in=" + UPDATED_LANGUAGE);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByLanguageIsNullOrNotNull() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where language is not null
        defaultNotificationTemplateFiltering("language.specified=true", "language.specified=false");
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByLanguageContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where language contains
        defaultNotificationTemplateFiltering("language.contains=" + DEFAULT_LANGUAGE, "language.contains=" + UPDATED_LANGUAGE);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByLanguageNotContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where language does not contain
        defaultNotificationTemplateFiltering("language.doesNotContain=" + UPDATED_LANGUAGE, "language.doesNotContain=" + DEFAULT_LANGUAGE);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySubjectIsEqualToSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where subject equals to
        defaultNotificationTemplateFiltering("subject.equals=" + DEFAULT_SUBJECT, "subject.equals=" + UPDATED_SUBJECT);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySubjectIsInShouldWork() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where subject in
        defaultNotificationTemplateFiltering("subject.in=" + DEFAULT_SUBJECT + "," + UPDATED_SUBJECT, "subject.in=" + UPDATED_SUBJECT);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySubjectIsNullOrNotNull() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where subject is not null
        defaultNotificationTemplateFiltering("subject.specified=true", "subject.specified=false");
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySubjectContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where subject contains
        defaultNotificationTemplateFiltering("subject.contains=" + DEFAULT_SUBJECT, "subject.contains=" + UPDATED_SUBJECT);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySubjectNotContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where subject does not contain
        defaultNotificationTemplateFiltering("subject.doesNotContain=" + UPDATED_SUBJECT, "subject.doesNotContain=" + DEFAULT_SUBJECT);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySmsTemplateIsEqualToSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where smsTemplate equals to
        defaultNotificationTemplateFiltering("smsTemplate.equals=" + DEFAULT_SMS_TEMPLATE, "smsTemplate.equals=" + UPDATED_SMS_TEMPLATE);
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySmsTemplateIsInShouldWork() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where smsTemplate in
        defaultNotificationTemplateFiltering(
            "smsTemplate.in=" + DEFAULT_SMS_TEMPLATE + "," + UPDATED_SMS_TEMPLATE,
            "smsTemplate.in=" + UPDATED_SMS_TEMPLATE
        );
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySmsTemplateIsNullOrNotNull() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where smsTemplate is not null
        defaultNotificationTemplateFiltering("smsTemplate.specified=true", "smsTemplate.specified=false");
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySmsTemplateContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where smsTemplate contains
        defaultNotificationTemplateFiltering(
            "smsTemplate.contains=" + DEFAULT_SMS_TEMPLATE,
            "smsTemplate.contains=" + UPDATED_SMS_TEMPLATE
        );
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesBySmsTemplateNotContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where smsTemplate does not contain
        defaultNotificationTemplateFiltering(
            "smsTemplate.doesNotContain=" + UPDATED_SMS_TEMPLATE,
            "smsTemplate.doesNotContain=" + DEFAULT_SMS_TEMPLATE
        );
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByPushTemplateIsEqualToSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where pushTemplate equals to
        defaultNotificationTemplateFiltering(
            "pushTemplate.equals=" + DEFAULT_PUSH_TEMPLATE,
            "pushTemplate.equals=" + UPDATED_PUSH_TEMPLATE
        );
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByPushTemplateIsInShouldWork() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where pushTemplate in
        defaultNotificationTemplateFiltering(
            "pushTemplate.in=" + DEFAULT_PUSH_TEMPLATE + "," + UPDATED_PUSH_TEMPLATE,
            "pushTemplate.in=" + UPDATED_PUSH_TEMPLATE
        );
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByPushTemplateIsNullOrNotNull() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where pushTemplate is not null
        defaultNotificationTemplateFiltering("pushTemplate.specified=true", "pushTemplate.specified=false");
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByPushTemplateContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where pushTemplate contains
        defaultNotificationTemplateFiltering(
            "pushTemplate.contains=" + DEFAULT_PUSH_TEMPLATE,
            "pushTemplate.contains=" + UPDATED_PUSH_TEMPLATE
        );
    }

    @Test
    @Transactional
    void getAllNotificationTemplatesByPushTemplateNotContainsSomething() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        // Get all the notificationTemplateList where pushTemplate does not contain
        defaultNotificationTemplateFiltering(
            "pushTemplate.doesNotContain=" + UPDATED_PUSH_TEMPLATE,
            "pushTemplate.doesNotContain=" + DEFAULT_PUSH_TEMPLATE
        );
    }

    private void defaultNotificationTemplateFiltering(String shouldBeFound, String shouldNotBeFound) throws Exception {
        defaultNotificationTemplateShouldBeFound(shouldBeFound);
        defaultNotificationTemplateShouldNotBeFound(shouldNotBeFound);
    }

    /**
     * Executes the search, and checks that the default entity is returned.
     */
    private void defaultNotificationTemplateShouldBeFound(String filter) throws Exception {
        restNotificationTemplateMockMvc
            .perform(get(ENTITY_API_URL + "?sort=id,desc&" + filter))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.[*].id").value(hasItem(notificationTemplate.getId().intValue())))
            .andExpect(jsonPath("$.[*].type").value(hasItem(DEFAULT_TYPE)))
            .andExpect(jsonPath("$.[*].language").value(hasItem(DEFAULT_LANGUAGE)))
            .andExpect(jsonPath("$.[*].subject").value(hasItem(DEFAULT_SUBJECT)))
            .andExpect(jsonPath("$.[*].emailBody").value(hasItem(DEFAULT_EMAIL_BODY)))
            .andExpect(jsonPath("$.[*].smsTemplate").value(hasItem(DEFAULT_SMS_TEMPLATE)))
            .andExpect(jsonPath("$.[*].pushTemplate").value(hasItem(DEFAULT_PUSH_TEMPLATE)));

        // Check, that the count call also returns 1
        restNotificationTemplateMockMvc
            .perform(get(ENTITY_API_URL + "/count?sort=id,desc&" + filter))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(content().string("1"));
    }

    /**
     * Executes the search, and checks that the default entity is not returned.
     */
    private void defaultNotificationTemplateShouldNotBeFound(String filter) throws Exception {
        restNotificationTemplateMockMvc
            .perform(get(ENTITY_API_URL + "?sort=id,desc&" + filter))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$").isArray())
            .andExpect(jsonPath("$").isEmpty());

        // Check, that the count call also returns 0
        restNotificationTemplateMockMvc
            .perform(get(ENTITY_API_URL + "/count?sort=id,desc&" + filter))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(content().string("0"));
    }

    @Test
    @Transactional
    void getNonExistingNotificationTemplate() throws Exception {
        // Get the notificationTemplate
        restNotificationTemplateMockMvc.perform(get(ENTITY_API_URL_ID, Long.MAX_VALUE)).andExpect(status().isNotFound());
    }

    @Test
    @Transactional
    void putExistingNotificationTemplate() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the notificationTemplate
        NotificationTemplate updatedNotificationTemplate = notificationTemplateRepository
            .findById(notificationTemplate.getId())
            .orElseThrow();
        // Disconnect from session so that the updates on updatedNotificationTemplate are not directly saved in db
        em.detach(updatedNotificationTemplate);
        updatedNotificationTemplate
            .type(UPDATED_TYPE)
            .language(UPDATED_LANGUAGE)
            .subject(UPDATED_SUBJECT)
            .emailBody(UPDATED_EMAIL_BODY)
            .smsTemplate(UPDATED_SMS_TEMPLATE)
            .pushTemplate(UPDATED_PUSH_TEMPLATE);
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(updatedNotificationTemplate);

        restNotificationTemplateMockMvc
            .perform(
                put(ENTITY_API_URL_ID, notificationTemplateDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isOk());

        // Validate the NotificationTemplate in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertPersistedNotificationTemplateToMatchAllProperties(updatedNotificationTemplate);
    }

    @Test
    @Transactional
    void putNonExistingNotificationTemplate() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        notificationTemplate.setId(longCount.incrementAndGet());

        // Create the NotificationTemplate
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restNotificationTemplateMockMvc
            .perform(
                put(ENTITY_API_URL_ID, notificationTemplateDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the NotificationTemplate in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithIdMismatchNotificationTemplate() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        notificationTemplate.setId(longCount.incrementAndGet());

        // Create the NotificationTemplate
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restNotificationTemplateMockMvc
            .perform(
                put(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the NotificationTemplate in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithMissingIdPathParamNotificationTemplate() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        notificationTemplate.setId(longCount.incrementAndGet());

        // Create the NotificationTemplate
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restNotificationTemplateMockMvc
            .perform(
                put(ENTITY_API_URL)
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isMethodNotAllowed());

        // Validate the NotificationTemplate in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void partialUpdateNotificationTemplateWithPatch() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the notificationTemplate using partial update
        NotificationTemplate partialUpdatedNotificationTemplate = new NotificationTemplate();
        partialUpdatedNotificationTemplate.setId(notificationTemplate.getId());

        partialUpdatedNotificationTemplate
            .type(UPDATED_TYPE)
            .subject(UPDATED_SUBJECT)
            .emailBody(UPDATED_EMAIL_BODY)
            .pushTemplate(UPDATED_PUSH_TEMPLATE);

        restNotificationTemplateMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedNotificationTemplate.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedNotificationTemplate))
            )
            .andExpect(status().isOk());

        // Validate the NotificationTemplate in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertNotificationTemplateUpdatableFieldsEquals(
            createUpdateProxyForBean(partialUpdatedNotificationTemplate, notificationTemplate),
            getPersistedNotificationTemplate(notificationTemplate)
        );
    }

    @Test
    @Transactional
    void fullUpdateNotificationTemplateWithPatch() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the notificationTemplate using partial update
        NotificationTemplate partialUpdatedNotificationTemplate = new NotificationTemplate();
        partialUpdatedNotificationTemplate.setId(notificationTemplate.getId());

        partialUpdatedNotificationTemplate
            .type(UPDATED_TYPE)
            .language(UPDATED_LANGUAGE)
            .subject(UPDATED_SUBJECT)
            .emailBody(UPDATED_EMAIL_BODY)
            .smsTemplate(UPDATED_SMS_TEMPLATE)
            .pushTemplate(UPDATED_PUSH_TEMPLATE);

        restNotificationTemplateMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedNotificationTemplate.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedNotificationTemplate))
            )
            .andExpect(status().isOk());

        // Validate the NotificationTemplate in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertNotificationTemplateUpdatableFieldsEquals(
            partialUpdatedNotificationTemplate,
            getPersistedNotificationTemplate(partialUpdatedNotificationTemplate)
        );
    }

    @Test
    @Transactional
    void patchNonExistingNotificationTemplate() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        notificationTemplate.setId(longCount.incrementAndGet());

        // Create the NotificationTemplate
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restNotificationTemplateMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, notificationTemplateDTO.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the NotificationTemplate in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithIdMismatchNotificationTemplate() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        notificationTemplate.setId(longCount.incrementAndGet());

        // Create the NotificationTemplate
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restNotificationTemplateMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the NotificationTemplate in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithMissingIdPathParamNotificationTemplate() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        notificationTemplate.setId(longCount.incrementAndGet());

        // Create the NotificationTemplate
        NotificationTemplateDTO notificationTemplateDTO = notificationTemplateMapper.toDto(notificationTemplate);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restNotificationTemplateMockMvc
            .perform(
                patch(ENTITY_API_URL)
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(notificationTemplateDTO))
            )
            .andExpect(status().isMethodNotAllowed());

        // Validate the NotificationTemplate in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void deleteNotificationTemplate() throws Exception {
        // Initialize the database
        insertedNotificationTemplate = notificationTemplateRepository.saveAndFlush(notificationTemplate);

        long databaseSizeBeforeDelete = getRepositoryCount();

        // Delete the notificationTemplate
        restNotificationTemplateMockMvc
            .perform(delete(ENTITY_API_URL_ID, notificationTemplate.getId()).with(csrf()).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isNoContent());

        // Validate the database contains one less item
        assertDecrementedRepositoryCount(databaseSizeBeforeDelete);
    }

    protected long getRepositoryCount() {
        return notificationTemplateRepository.count();
    }

    protected void assertIncrementedRepositoryCount(long countBefore) {
        assertThat(countBefore + 1).isEqualTo(getRepositoryCount());
    }

    protected void assertDecrementedRepositoryCount(long countBefore) {
        assertThat(countBefore - 1).isEqualTo(getRepositoryCount());
    }

    protected void assertSameRepositoryCount(long countBefore) {
        assertThat(countBefore).isEqualTo(getRepositoryCount());
    }

    protected NotificationTemplate getPersistedNotificationTemplate(NotificationTemplate notificationTemplate) {
        return notificationTemplateRepository.findById(notificationTemplate.getId()).orElseThrow();
    }

    protected void assertPersistedNotificationTemplateToMatchAllProperties(NotificationTemplate expectedNotificationTemplate) {
        assertNotificationTemplateAllPropertiesEquals(
            expectedNotificationTemplate,
            getPersistedNotificationTemplate(expectedNotificationTemplate)
        );
    }

    protected void assertPersistedNotificationTemplateToMatchUpdatableProperties(NotificationTemplate expectedNotificationTemplate) {
        assertNotificationTemplateAllUpdatablePropertiesEquals(
            expectedNotificationTemplate,
            getPersistedNotificationTemplate(expectedNotificationTemplate)
        );
    }
}
