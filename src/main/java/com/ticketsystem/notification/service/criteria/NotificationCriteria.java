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

    private LongFilter id;

    private UUIDFilter recipientId;

    private StringFilter templateType;

    private StringFilter templateLanguage;

    private StringFilter channel;

    private StringFilter metadata;

    private InstantFilter sentAt;

    private InstantFilter deliveredAt;

    private InstantFilter readAt;

    private StringFilter status;

    private UUIDFilter bookingId;

    private LongFilter templateId;

    private Boolean distinct;

    public NotificationCriteria() {}

    public NotificationCriteria(NotificationCriteria other) {
        this.id = other.optionalId().map(LongFilter::copy).orElse(null);
        this.recipientId = other.optionalRecipientId().map(UUIDFilter::copy).orElse(null);
        this.templateType = other.optionalTemplateType().map(StringFilter::copy).orElse(null);
        this.templateLanguage = other.optionalTemplateLanguage().map(StringFilter::copy).orElse(null);
        this.channel = other.optionalChannel().map(StringFilter::copy).orElse(null);
        this.metadata = other.optionalMetadata().map(StringFilter::copy).orElse(null);
        this.sentAt = other.optionalSentAt().map(InstantFilter::copy).orElse(null);
        this.deliveredAt = other.optionalDeliveredAt().map(InstantFilter::copy).orElse(null);
        this.readAt = other.optionalReadAt().map(InstantFilter::copy).orElse(null);
        this.status = other.optionalStatus().map(StringFilter::copy).orElse(null);
        this.bookingId = other.optionalBookingId().map(UUIDFilter::copy).orElse(null);
        this.templateId = other.optionalTemplateId().map(LongFilter::copy).orElse(null);
        this.distinct = other.distinct;
    }

    @Override
    public NotificationCriteria copy() {
        return new NotificationCriteria(this);
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

    public StringFilter getTemplateType() {
        return templateType;
    }

    public Optional<StringFilter> optionalTemplateType() {
        return Optional.ofNullable(templateType);
    }

    public StringFilter templateType() {
        if (templateType == null) {
            setTemplateType(new StringFilter());
        }
        return templateType;
    }

    public void setTemplateType(StringFilter templateType) {
        this.templateType = templateType;
    }

    public StringFilter getTemplateLanguage() {
        return templateLanguage;
    }

    public Optional<StringFilter> optionalTemplateLanguage() {
        return Optional.ofNullable(templateLanguage);
    }

    public StringFilter templateLanguage() {
        if (templateLanguage == null) {
            setTemplateLanguage(new StringFilter());
        }
        return templateLanguage;
    }

    public void setTemplateLanguage(StringFilter templateLanguage) {
        this.templateLanguage = templateLanguage;
    }

    public StringFilter getChannel() {
        return channel;
    }

    public Optional<StringFilter> optionalChannel() {
        return Optional.ofNullable(channel);
    }

    public StringFilter channel() {
        if (channel == null) {
            setChannel(new StringFilter());
        }
        return channel;
    }

    public void setChannel(StringFilter channel) {
        this.channel = channel;
    }

    public StringFilter getMetadata() {
        return metadata;
    }

    public Optional<StringFilter> optionalMetadata() {
        return Optional.ofNullable(metadata);
    }

    public StringFilter metadata() {
        if (metadata == null) {
            setMetadata(new StringFilter());
        }
        return metadata;
    }

    public void setMetadata(StringFilter metadata) {
        this.metadata = metadata;
    }

    public InstantFilter getSentAt() {
        return sentAt;
    }

    public Optional<InstantFilter> optionalSentAt() {
        return Optional.ofNullable(sentAt);
    }

    public InstantFilter sentAt() {
        if (sentAt == null) {
            setSentAt(new InstantFilter());
        }
        return sentAt;
    }

    public void setSentAt(InstantFilter sentAt) {
        this.sentAt = sentAt;
    }

    public InstantFilter getDeliveredAt() {
        return deliveredAt;
    }

    public Optional<InstantFilter> optionalDeliveredAt() {
        return Optional.ofNullable(deliveredAt);
    }

    public InstantFilter deliveredAt() {
        if (deliveredAt == null) {
            setDeliveredAt(new InstantFilter());
        }
        return deliveredAt;
    }

    public void setDeliveredAt(InstantFilter deliveredAt) {
        this.deliveredAt = deliveredAt;
    }

    public InstantFilter getReadAt() {
        return readAt;
    }

    public Optional<InstantFilter> optionalReadAt() {
        return Optional.ofNullable(readAt);
    }

    public InstantFilter readAt() {
        if (readAt == null) {
            setReadAt(new InstantFilter());
        }
        return readAt;
    }

    public void setReadAt(InstantFilter readAt) {
        this.readAt = readAt;
    }

    public StringFilter getStatus() {
        return status;
    }

    public Optional<StringFilter> optionalStatus() {
        return Optional.ofNullable(status);
    }

    public StringFilter status() {
        if (status == null) {
            setStatus(new StringFilter());
        }
        return status;
    }

    public void setStatus(StringFilter status) {
        this.status = status;
    }

    public UUIDFilter getBookingId() {
        return bookingId;
    }

    public Optional<UUIDFilter> optionalBookingId() {
        return Optional.ofNullable(bookingId);
    }

    public UUIDFilter bookingId() {
        if (bookingId == null) {
            setBookingId(new UUIDFilter());
        }
        return bookingId;
    }

    public void setBookingId(UUIDFilter bookingId) {
        this.bookingId = bookingId;
    }

    public LongFilter getTemplateId() {
        return templateId;
    }

    public Optional<LongFilter> optionalTemplateId() {
        return Optional.ofNullable(templateId);
    }

    public LongFilter templateId() {
        if (templateId == null) {
            setTemplateId(new LongFilter());
        }
        return templateId;
    }

    public void setTemplateId(LongFilter templateId) {
        this.templateId = templateId;
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
            Objects.equals(templateType, that.templateType) &&
            Objects.equals(templateLanguage, that.templateLanguage) &&
            Objects.equals(channel, that.channel) &&
            Objects.equals(metadata, that.metadata) &&
            Objects.equals(sentAt, that.sentAt) &&
            Objects.equals(deliveredAt, that.deliveredAt) &&
            Objects.equals(readAt, that.readAt) &&
            Objects.equals(status, that.status) &&
            Objects.equals(bookingId, that.bookingId) &&
            Objects.equals(templateId, that.templateId) &&
            Objects.equals(distinct, that.distinct)
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            recipientId,
            templateType,
            templateLanguage,
            channel,
            metadata,
            sentAt,
            deliveredAt,
            readAt,
            status,
            bookingId,
            templateId,
            distinct
        );
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "NotificationCriteria{" +
            optionalId().map(f -> "id=" + f + ", ").orElse("") +
            optionalRecipientId().map(f -> "recipientId=" + f + ", ").orElse("") +
            optionalTemplateType().map(f -> "templateType=" + f + ", ").orElse("") +
            optionalTemplateLanguage().map(f -> "templateLanguage=" + f + ", ").orElse("") +
            optionalChannel().map(f -> "channel=" + f + ", ").orElse("") +
            optionalMetadata().map(f -> "metadata=" + f + ", ").orElse("") +
            optionalSentAt().map(f -> "sentAt=" + f + ", ").orElse("") +
            optionalDeliveredAt().map(f -> "deliveredAt=" + f + ", ").orElse("") +
            optionalReadAt().map(f -> "readAt=" + f + ", ").orElse("") +
            optionalStatus().map(f -> "status=" + f + ", ").orElse("") +
            optionalBookingId().map(f -> "bookingId=" + f + ", ").orElse("") +
            optionalTemplateId().map(f -> "templateId=" + f + ", ").orElse("") +
            optionalDistinct().map(f -> "distinct=" + f + ", ").orElse("") +
        "}";
    }
}
