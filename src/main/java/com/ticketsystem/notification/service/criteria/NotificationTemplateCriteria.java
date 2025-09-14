package com.ticketsystem.notification.service.criteria;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import org.springdoc.core.annotations.ParameterObject;
import tech.jhipster.service.Criteria;
import tech.jhipster.service.filter.*;

/**
 * Criteria class for the {@link com.ticketsystem.notification.domain.NotificationTemplate} entity. This class is used
 * in {@link com.ticketsystem.notification.web.rest.NotificationTemplateResource} to receive all the possible filtering options from
 * the Http GET request parameters.
 * For example the following could be a valid request:
 * {@code /notification-templates?id.greaterThan=5&attr1.contains=something&attr2.specified=false}
 * As Spring is unable to properly convert the types, unless specific {@link Filter} class are used, we need to use
 * fix type specific filters.
 */
@ParameterObject
@SuppressWarnings("common-java:DuplicatedBlocks")
public class NotificationTemplateCriteria implements Serializable, Criteria {

    private static final long serialVersionUID = 1L;

    private LongFilter id;

    private StringFilter type;

    private StringFilter language;

    private StringFilter subject;

    private StringFilter smsTemplate;

    private StringFilter pushTemplate;

    private Boolean distinct;

    public NotificationTemplateCriteria() {}

    public NotificationTemplateCriteria(NotificationTemplateCriteria other) {
        this.id = other.optionalId().map(LongFilter::copy).orElse(null);
        this.type = other.optionalType().map(StringFilter::copy).orElse(null);
        this.language = other.optionalLanguage().map(StringFilter::copy).orElse(null);
        this.subject = other.optionalSubject().map(StringFilter::copy).orElse(null);
        this.smsTemplate = other.optionalSmsTemplate().map(StringFilter::copy).orElse(null);
        this.pushTemplate = other.optionalPushTemplate().map(StringFilter::copy).orElse(null);
        this.distinct = other.distinct;
    }

    @Override
    public NotificationTemplateCriteria copy() {
        return new NotificationTemplateCriteria(this);
    }

    public LongFilter getId() {
        return id;
    }

    public Optional<LongFilter> optionalId() {
        return Optional.ofNullable(id);
    }

    public LongFilter id() {
        if (id == null) {
            setId(new LongFilter());
        }
        return id;
    }

    public void setId(LongFilter id) {
        this.id = id;
    }

    public StringFilter getType() {
        return type;
    }

    public Optional<StringFilter> optionalType() {
        return Optional.ofNullable(type);
    }

    public StringFilter type() {
        if (type == null) {
            setType(new StringFilter());
        }
        return type;
    }

    public void setType(StringFilter type) {
        this.type = type;
    }

    public StringFilter getLanguage() {
        return language;
    }

    public Optional<StringFilter> optionalLanguage() {
        return Optional.ofNullable(language);
    }

    public StringFilter language() {
        if (language == null) {
            setLanguage(new StringFilter());
        }
        return language;
    }

    public void setLanguage(StringFilter language) {
        this.language = language;
    }

    public StringFilter getSubject() {
        return subject;
    }

    public Optional<StringFilter> optionalSubject() {
        return Optional.ofNullable(subject);
    }

    public StringFilter subject() {
        if (subject == null) {
            setSubject(new StringFilter());
        }
        return subject;
    }

    public void setSubject(StringFilter subject) {
        this.subject = subject;
    }

    public StringFilter getSmsTemplate() {
        return smsTemplate;
    }

    public Optional<StringFilter> optionalSmsTemplate() {
        return Optional.ofNullable(smsTemplate);
    }

    public StringFilter smsTemplate() {
        if (smsTemplate == null) {
            setSmsTemplate(new StringFilter());
        }
        return smsTemplate;
    }

    public void setSmsTemplate(StringFilter smsTemplate) {
        this.smsTemplate = smsTemplate;
    }

    public StringFilter getPushTemplate() {
        return pushTemplate;
    }

    public Optional<StringFilter> optionalPushTemplate() {
        return Optional.ofNullable(pushTemplate);
    }

    public StringFilter pushTemplate() {
        if (pushTemplate == null) {
            setPushTemplate(new StringFilter());
        }
        return pushTemplate;
    }

    public void setPushTemplate(StringFilter pushTemplate) {
        this.pushTemplate = pushTemplate;
    }

    public Boolean getDistinct() {
        return distinct;
    }

    public Optional<Boolean> optionalDistinct() {
        return Optional.ofNullable(distinct);
    }

    public Boolean distinct() {
        if (distinct == null) {
            setDistinct(true);
        }
        return distinct;
    }

    public void setDistinct(Boolean distinct) {
        this.distinct = distinct;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NotificationTemplateCriteria that = (NotificationTemplateCriteria) o;
        return (
            Objects.equals(id, that.id) &&
            Objects.equals(type, that.type) &&
            Objects.equals(language, that.language) &&
            Objects.equals(subject, that.subject) &&
            Objects.equals(smsTemplate, that.smsTemplate) &&
            Objects.equals(pushTemplate, that.pushTemplate) &&
            Objects.equals(distinct, that.distinct)
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, language, subject, smsTemplate, pushTemplate, distinct);
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "NotificationTemplateCriteria{" +
            optionalId().map(f -> "id=" + f + ", ").orElse("") +
            optionalType().map(f -> "type=" + f + ", ").orElse("") +
            optionalLanguage().map(f -> "language=" + f + ", ").orElse("") +
            optionalSubject().map(f -> "subject=" + f + ", ").orElse("") +
            optionalSmsTemplate().map(f -> "smsTemplate=" + f + ", ").orElse("") +
            optionalPushTemplate().map(f -> "pushTemplate=" + f + ", ").orElse("") +
            optionalDistinct().map(f -> "distinct=" + f + ", ").orElse("") +
        "}";
    }
}
