package com.ticketsystem.notification.service.criteria;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import org.springdoc.core.annotations.ParameterObject;
import tech.jhipster.service.Criteria;
import tech.jhipster.service.filter.*;

/**
 * Criteria class for the {@link com.ticketsystem.notification.domain.Notification} entity. This class is used
 * in {@link com.ticketsystem.notification.web.rest.NotificationResource} to receive all the possible filtering options from
 * the Http GET request parameters.
 * For example the following could be a valid request:
 * {@code /notifications?id.greaterThan=5&attr1.contains=something&attr2.specified=false}
 * As Spring is unable to properly convert the types, unless specific {@link Filter} class are used, we need to use
 * fix type specific filters.
 */
@ParameterObject
@SuppressWarnings("common-java:DuplicatedBlocks")
public class NotificationCriteria implements Serializable, Criteria {

    private static final long serialVersionUID = 1L;

    private UUIDFilter id;

    private UUIDFilter recipientId;

    private StringFilter type;

    private StringFilter title;

    private BooleanFilter isRead;

    private StringFilter relatedEntityType;

    private UUIDFilter relatedEntityId;

    private InstantFilter createdAt;

    private InstantFilter scheduledAt;

    private Boolean distinct;

    public NotificationCriteria() {}

    public NotificationCriteria(NotificationCriteria other) {
        this.id = other.optionalId().map(UUIDFilter::copy).orElse(null);
        this.recipientId = other.optionalRecipientId().map(UUIDFilter::copy).orElse(null);
        this.type = other.optionalType().map(StringFilter::copy).orElse(null);
        this.title = other.optionalTitle().map(StringFilter::copy).orElse(null);
        this.isRead = other.optionalIsRead().map(BooleanFilter::copy).orElse(null);
        this.relatedEntityType = other.optionalRelatedEntityType().map(StringFilter::copy).orElse(null);
        this.relatedEntityId = other.optionalRelatedEntityId().map(UUIDFilter::copy).orElse(null);
        this.createdAt = other.optionalCreatedAt().map(InstantFilter::copy).orElse(null);
        this.scheduledAt = other.optionalScheduledAt().map(InstantFilter::copy).orElse(null);
        this.distinct = other.distinct;
    }

    @Override
    public NotificationCriteria copy() {
        return new NotificationCriteria(this);
    }

    public UUIDFilter getId() {
        return id;
    }

    public Optional<UUIDFilter> optionalId() {
        return Optional.ofNullable(id);
    }

    public UUIDFilter id() {
        if (id == null) {
            setId(new UUIDFilter());
        }
        return id;
    }

    public void setId(UUIDFilter id) {
        this.id = id;
    }

    public UUIDFilter getRecipientId() {
        return recipientId;
    }

    public Optional<UUIDFilter> optionalRecipientId() {
        return Optional.ofNullable(recipientId);
    }

    public UUIDFilter recipientId() {
        if (recipientId == null) {
            setRecipientId(new UUIDFilter());
        }
        return recipientId;
    }

    public void setRecipientId(UUIDFilter recipientId) {
        this.recipientId = recipientId;
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

    public StringFilter getTitle() {
        return title;
    }

    public Optional<StringFilter> optionalTitle() {
        return Optional.ofNullable(title);
    }

    public StringFilter title() {
        if (title == null) {
            setTitle(new StringFilter());
        }
        return title;
    }

    public void setTitle(StringFilter title) {
        this.title = title;
    }

    public BooleanFilter getIsRead() {
        return isRead;
    }

    public Optional<BooleanFilter> optionalIsRead() {
        return Optional.ofNullable(isRead);
    }

    public BooleanFilter isRead() {
        if (isRead == null) {
            setIsRead(new BooleanFilter());
        }
        return isRead;
    }

    public void setIsRead(BooleanFilter isRead) {
        this.isRead = isRead;
    }

    public StringFilter getRelatedEntityType() {
        return relatedEntityType;
    }

    public Optional<StringFilter> optionalRelatedEntityType() {
        return Optional.ofNullable(relatedEntityType);
    }

    public StringFilter relatedEntityType() {
        if (relatedEntityType == null) {
            setRelatedEntityType(new StringFilter());
        }
        return relatedEntityType;
    }

    public void setRelatedEntityType(StringFilter relatedEntityType) {
        this.relatedEntityType = relatedEntityType;
    }

    public UUIDFilter getRelatedEntityId() {
        return relatedEntityId;
    }

    public Optional<UUIDFilter> optionalRelatedEntityId() {
        return Optional.ofNullable(relatedEntityId);
    }

    public UUIDFilter relatedEntityId() {
        if (relatedEntityId == null) {
            setRelatedEntityId(new UUIDFilter());
        }
        return relatedEntityId;
    }

    public void setRelatedEntityId(UUIDFilter relatedEntityId) {
        this.relatedEntityId = relatedEntityId;
    }

    public InstantFilter getCreatedAt() {
        return createdAt;
    }

    public Optional<InstantFilter> optionalCreatedAt() {
        return Optional.ofNullable(createdAt);
    }

    public InstantFilter createdAt() {
        if (createdAt == null) {
            setCreatedAt(new InstantFilter());
        }
        return createdAt;
    }

    public void setCreatedAt(InstantFilter createdAt) {
        this.createdAt = createdAt;
    }

    public InstantFilter getScheduledAt() {
        return scheduledAt;
    }

    public Optional<InstantFilter> optionalScheduledAt() {
        return Optional.ofNullable(scheduledAt);
    }

    public InstantFilter scheduledAt() {
        if (scheduledAt == null) {
            setScheduledAt(new InstantFilter());
        }
        return scheduledAt;
    }

    public void setScheduledAt(InstantFilter scheduledAt) {
        this.scheduledAt = scheduledAt;
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
        final NotificationCriteria that = (NotificationCriteria) o;
        return (
            Objects.equals(id, that.id) &&
            Objects.equals(recipientId, that.recipientId) &&
            Objects.equals(type, that.type) &&
            Objects.equals(title, that.title) &&
            Objects.equals(isRead, that.isRead) &&
            Objects.equals(relatedEntityType, that.relatedEntityType) &&
            Objects.equals(relatedEntityId, that.relatedEntityId) &&
            Objects.equals(createdAt, that.createdAt) &&
            Objects.equals(scheduledAt, that.scheduledAt) &&
            Objects.equals(distinct, that.distinct)
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, recipientId, type, title, isRead, relatedEntityType, relatedEntityId, createdAt, scheduledAt, distinct);
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "NotificationCriteria{" +
            optionalId().map(f -> "id=" + f + ", ").orElse("") +
            optionalRecipientId().map(f -> "recipientId=" + f + ", ").orElse("") +
            optionalType().map(f -> "type=" + f + ", ").orElse("") +
            optionalTitle().map(f -> "title=" + f + ", ").orElse("") +
            optionalIsRead().map(f -> "isRead=" + f + ", ").orElse("") +
            optionalRelatedEntityType().map(f -> "relatedEntityType=" + f + ", ").orElse("") +
            optionalRelatedEntityId().map(f -> "relatedEntityId=" + f + ", ").orElse("") +
            optionalCreatedAt().map(f -> "createdAt=" + f + ", ").orElse("") +
            optionalScheduledAt().map(f -> "scheduledAt=" + f + ", ").orElse("") +
            optionalDistinct().map(f -> "distinct=" + f + ", ").orElse("") +
        "}";
    }
}
