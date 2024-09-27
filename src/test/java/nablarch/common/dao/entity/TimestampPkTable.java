package nablarch.common.dao.entity;

import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

/**
 *
 */
@Entity
@Table(name = "timestamp_pk_table")
public class TimestampPkTable {
    
    @Id
    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "timestamp_col")
    public Date timestampCol;

    @Column(name = "name")
    public String name;

    public TimestampPkTable() {
    }

    public TimestampPkTable(final Date timestampCol, final String name) {
        this.timestampCol = timestampCol;
        this.name = name;
    }

    @Id
    @Temporal(TemporalType.DATE)
    public Date getTimestampCol() {
        return timestampCol;
    }

    public void setTimestampCol(final Date timestampCol) {
        this.timestampCol = timestampCol;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }
}
