package nablarch.common.dao.entity;

import jakarta.persistence.Access;
import jakarta.persistence.AccessType;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

@Entity
@Access(AccessType.FIELD)
public class IdentityColumnEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    public IdentityColumnEntity() {
    }
    
    public Long getId() {
        return id;
    }

    public void setId(final Long id) {
        this.id = id;
    }
}
