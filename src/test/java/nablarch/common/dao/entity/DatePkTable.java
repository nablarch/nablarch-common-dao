package nablarch.common.dao.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * 日付(DATE)が主キーのエンティティ
 */
@Entity
@Table(name = "date_pk_table")
public class DatePkTable {
    
    @Column(name = "date_col")
    @Id
    @Temporal(TemporalType.DATE)
    public Date dateCol;
    
    @Column(name = "name")
    public String name;

    public DatePkTable() {
    }

    public DatePkTable(final Date dateCol, final String name) {
        this.dateCol = dateCol;
        this.name = name;
    }

    @Temporal(TemporalType.DATE)
    @Id
    public Date getDateCol() {
        return dateCol;
    }

    public void setDateCol(final Date dateCol) {
        this.dateCol = dateCol;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }
}
