package com.ticketsystem.notification.domain;

import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * A Notification.
 */
@Entity
@Table(name = "notification")
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@SuppressWarnings("common-java:DuplicatedBlocks")
public class Notification implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @NotNull
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Column(name = "recipient_id", length = 36, nullable = false)
    private UUID recipientId;

    @Column(name = "template_type")
    private String templateType;

    @Column(name = "template_language")
    private String templateLanguage;

    @NotNull
    @Column(name = "channel", nullable = false)
    private String channel;

    @Lob
    @Column(name = "content")
    private String content;

    @Column(name = "metadata")
    private String metadata;

    @Column(name = "sent_at")
    private Instant sentAt;

    @Column(name = "delivered_at")
    private Instant deliveredAt;

    @Column(name = "read_at")
    private Instant readAt;

    @Column(name = "status")
    private String status;

    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Column(name = "booking_id", length = 36)
    private UUID bookingId;

    @ManyToOne(fetch = FetchType.LAZY)
    private NotificationTemplate template;

    // jhipster-needle-entity-add-field - JHipster will add fields here

    public Long getId() {
        return this.id;
    }

    public Notification id(Long id) {
        this.setId(id);
        return this;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public UUID getRecipientId() {
        return this.recipientId;
    }

    public Notification recipientId(UUID recipientId) {
        this.setRecipientId(recipientId);
        return this;
    }

    public void setRecipientId(UUID recipientId) {
        this.recipientId = recipientId;
    }

    public String getTemplateType() {
        return this.templateType;
    }

    public Notification templateType(String templateType) {
        this.setTemplateType(templateType);
        return this;
    }

    public void setTemplateType(String templateType) {
        this.templateType = templateType;
    }

    public String getTemplateLanguage() {
        return this.templateLanguage;
    }

    public Notification templateLanguage(String templateLanguage) {
        this.setTemplateLanguage(templateLanguage);
        return this;
    }

    public void setTemplateLanguage(String templateLanguage) {
        this.templateLanguage = templateLanguage;
    }

    public String getChannel() {
        return this.channel;
    }

    public Notification channel(String channel) {
        this.setChannel(channel);
        return this;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getContent() {
        return this.content;
    }

    public Notification content(String content) {
        this.setContent(content);
        return this;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getMetadata() {
        return this.metadata;
    }

    public Notification metadata(String metadata) {
        this.setMetadata(metadata);
        return this;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public Instant getSentAt() {
        return this.sentAt;
    }

    public Notification sentAt(Instant sentAt) {
        this.setSentAt(sentAt);
        return this;
    }

    public void setSentAt(Instant sentAt) {
        this.sentAt = sentAt;
    }

    public Instant getDeliveredAt() {
        return this.deliveredAt;
    }

    public Notification deliveredAt(Instant deliveredAt) {
        this.setDeliveredAt(deliveredAt);
        return this;
    }

    public void setDeliveredAt(Instant deliveredAt) {
        this.deliveredAt = deliveredAt;
    }

    public Instant getReadAt() {
        return this.readAt;
    }

    public Notification readAt(Instant readAt) {
        this.setReadAt(readAt);
        return this;
    }

    public void setReadAt(Instant readAt) {
        this.readAt = readAt;
    }

    public String getStatus() {
        return this.status;
    }

    public Notification status(String status) {
        this.setStatus(status);
        return this;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public UUID getBookingId() {
        return this.bookingId;
    }

    public Notification bookingId(UUID bookingId) {
        this.setBookingId(bookingId);
        return this;
    }

    public void setBookingId(UUID bookingId) {
        this.bookingId = bookingId;
    }

    public NotificationTemplate getTemplate() {
        return this.template;
    }

    public void setTemplate(NotificationTemplate notificationTemplate) {
        this.template = notificationTemplate;
    }

    public Notification template(NotificationTemplate notificationTemplate) {
        this.setTemplate(notificationTemplate);
        return this;
    }

    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and setters here

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Notification)) {
            return false;
        }
        return getId() != null && getId().equals(((Notification) o).getId());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Notification{" +
            "id=" + getId() +
            ", recipientId='" + getRecipientId() + "'" +
            ", templateType='" + getTemplateType() + "'" +
            ", templateLanguage='" + getTemplateLanguage() + "'" +
            ", channel='" + getChannel() + "'" +
            ", content='" + getContent() + "'" +
            ", metadata='" + getMetadata() + "'" +
            ", sentAt='" + getSentAt() + "'" +
            ", deliveredAt='" + getDeliveredAt() + "'" +
            ", readAt='" + getReadAt() + "'" +
            ", status='" + getStatus() + "'" +
            ", bookingId='" + getBookingId() + "'" +
            "}";
    }
}
