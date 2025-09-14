package com.ticketsystem.notification.domain;

import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import java.io.Serializable;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

/**
 * A NotificationTemplate.
 */
@Entity
@Table(name = "notification_template")
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@SuppressWarnings("common-java:DuplicatedBlocks")
public class NotificationTemplate implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @NotNull
    @Column(name = "type", nullable = false)
    private String type;

    @NotNull
    @Column(name = "language", nullable = false)
    private String language;

    @NotNull
    @Column(name = "subject", nullable = false)
    private String subject;

    @Lob
    @Column(name = "email_body")
    private String emailBody;

    @Column(name = "sms_template")
    private String smsTemplate;

    @Column(name = "push_template")
    private String pushTemplate;

    // jhipster-needle-entity-add-field - JHipster will add fields here

    public Long getId() {
        return this.id;
    }

    public NotificationTemplate id(Long id) {
        this.setId(id);
        return this;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getType() {
        return this.type;
    }

    public NotificationTemplate type(String type) {
        this.setType(type);
        return this;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLanguage() {
        return this.language;
    }

    public NotificationTemplate language(String language) {
        this.setLanguage(language);
        return this;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getSubject() {
        return this.subject;
    }

    public NotificationTemplate subject(String subject) {
        this.setSubject(subject);
        return this;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getEmailBody() {
        return this.emailBody;
    }

    public NotificationTemplate emailBody(String emailBody) {
        this.setEmailBody(emailBody);
        return this;
    }

    public void setEmailBody(String emailBody) {
        this.emailBody = emailBody;
    }

    public String getSmsTemplate() {
        return this.smsTemplate;
    }

    public NotificationTemplate smsTemplate(String smsTemplate) {
        this.setSmsTemplate(smsTemplate);
        return this;
    }

    public void setSmsTemplate(String smsTemplate) {
        this.smsTemplate = smsTemplate;
    }

    public String getPushTemplate() {
        return this.pushTemplate;
    }

    public NotificationTemplate pushTemplate(String pushTemplate) {
        this.setPushTemplate(pushTemplate);
        return this;
    }

    public void setPushTemplate(String pushTemplate) {
        this.pushTemplate = pushTemplate;
    }

    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and setters here

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NotificationTemplate)) {
            return false;
        }
        return getId() != null && getId().equals(((NotificationTemplate) o).getId());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "NotificationTemplate{" +
            "id=" + getId() +
            ", type='" + getType() + "'" +
            ", language='" + getLanguage() + "'" +
            ", subject='" + getSubject() + "'" +
            ", emailBody='" + getEmailBody() + "'" +
            ", smsTemplate='" + getSmsTemplate() + "'" +
            ", pushTemplate='" + getPushTemplate() + "'" +
            "}";
    }
}
