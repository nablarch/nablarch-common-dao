package nablarch.common.dao.entity;

import java.sql.Timestamp;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

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
