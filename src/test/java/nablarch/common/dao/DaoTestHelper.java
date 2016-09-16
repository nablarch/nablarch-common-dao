package nablarch.common.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.TableGenerator;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Version;

/**
 * daoのテストをサポートするクラス。
 *
 * @author hisaaki sioiri
 */
public class DaoTestHelper {

    /** USERSテーブルに対応したEntity */
    @Entity
    @Table(name = "DAO_USERS")
    public static class Users {

        @Id
        @Column(name = "USER_ID", length = 15)
        public Long id;

        @Column(name = "NAME", length = 100)
        public String name;

        @Column(name = "BIRTHDAY")
        @Temporal(TemporalType.DATE)
        public Date birthday;
        
        @Column(name = "INSERT_DATE")
        @Temporal(TemporalType.TIMESTAMP)
        public Date insertDate;

        @Column(name = "VERSION", length = 18)
        public Long version;

        @Column(name = "active")
        public Boolean active = Boolean.FALSE;

        public Users() {
        }

        public Users(Long id) {
            this.id = id;
        }

        public Users(Long id, String name, Date birthday, Date insertDate) {
            this.birthday = birthday;
            this.insertDate = insertDate;
            this.id = id;
            this.name = name;
        }

        public Users(Long id, String name, Date birthday, Date insertDate, Long version) {
            this.id = id;
            this.name = name;
            this.birthday = birthday;
            this.insertDate = insertDate;
            this.version = version;
        }

        public Users(Long id, String name, Date birthday, Date insertDate, Long version, boolean active) {
            this.id = id;
            this.name = name;
            this.birthday = birthday;
            this.insertDate = insertDate;
            this.version = version;
            this.active = active;
        }

        @Id
        @Column(name = "USER_ID", length = 15)
        @GeneratedValue(strategy = GenerationType.AUTO, generator = "seq")
        @SequenceGenerator(name = "seq", sequenceName = "USER_ID_SEQ")
        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Temporal(TemporalType.DATE)
        public Date getBirthday() {
            return birthday;
        }

        public void setBirthday(Date birthday) {
            this.birthday = birthday;
        }
        
        @Temporal(TemporalType.TIMESTAMP)
        public Date getInsertDate() {
            return insertDate;
        }

        public void setInsertDate(Date insertDate) {
            this.insertDate = insertDate;
        }

        @Version
        public Long getVersion() {
            return version;
        }

        public void setVersion(Long version) {
            this.version = version;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Users users = (Users) o;

            if (id != null ? !id.equals(users.id) : users.id != null) {
                return false;
            }
            if (name != null ? !name.equals(users.name) : users.name != null) {
                return false;
            }
            if (birthday != null ? !birthday.equals(users.birthday) : users.birthday != null) {
                return false;
            }
            if (insertDate != null ? !insertDate.equals(users.insertDate) : users.insertDate != null) {
                return false;
            }
            if (version != null ? !version.equals(users.version) : users.version != null) {
                return false;
            }
            return !(active != null ? !active.equals(users.active) : users.active != null);

        }

        @Override
        public int hashCode() {
            int result = id != null ? id.hashCode() : 0;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (birthday != null ? birthday.hashCode() : 0);
            result = 31 * result + (insertDate != null ? insertDate.hashCode() : 0);
            result = 31 * result + (version != null ? version.hashCode() : 0);
            result = 31 * result + (active != null ? active.hashCode() : 0);
            return result;
        }
    }

    /** USERSテーブルに対応したEntity(IDの採番方法がAUTO) */
    @Entity
    @Table(name = "DAO_USERS")
    public static class AutoGenUsers {

        private Long id;

        private String name;

        private Date birthday;
        
        private Date insertDate;

        private Long version;

        public AutoGenUsers() {
        }

        public AutoGenUsers(Long id, String name, Date birthday, Date insertDate) {
            this.birthday = birthday;
            this.insertDate = insertDate;
            this.id = id;
            this.name = name;
        }

        @Id
        @Column(name = "USER_ID")
        @GeneratedValue(strategy = GenerationType.AUTO)
        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Temporal(TemporalType.DATE)
        public Date getBirthday() {
            return birthday;
        }

        public void setBirthday(Date birthday) {
            this.birthday = birthday;
        }
        
        @Temporal(TemporalType.TIMESTAMP)
        public Date getInsertDate() {
            return insertDate;
        }

        public void setInsertDate(Date insertDate) {
            this.insertDate = insertDate;
        }

        @Version
        public Long getVersion() {
            return version;
        }

        public void setVersion(Long version) {
            this.version = version;
        }
    }

    /** USERSテーブルに対応したEntity(IDの採番方法がIDENTITY) */
    @Entity
    @Table(name = "DAO_USERS")
    public static class IdentityGenUsers {

        private Long id;

        private String name;

        private Date birthday;
        
        private Date insertDate;

        private Long version;

        public IdentityGenUsers() {
        }

        public IdentityGenUsers(Long id, String name, Date birthday, Date insertDate) {
            this.birthday = birthday;
            this.insertDate = insertDate;
            this.id = id;
            this.name = name;
        }

        @Id
        @Column(name = "USER_ID")
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Temporal(TemporalType.DATE)
        public Date getBirthday() {
            return birthday;
        }

        public void setBirthday(Date birthday) {
            this.birthday = birthday;
        }
        
        @Temporal(TemporalType.TIMESTAMP)
        public Date getInsertDate() {
            return insertDate;
        }

        public void setInsertDate(Date insertDate) {
            this.insertDate = insertDate;
        }

        @Version
        public Long getVersion() {
            return version;
        }

        public void setVersion(Long version) {
            this.version = version;
        }
    }

    @Entity
    @Table(name = "DAO_USERS2")
    public static class Users2 {

        @Id
        @Column(name = "USER_ID", length = 15)
        public Long id;

        @Column(name = "NAME", length = 100)
        public String name;

        @Column(name = "BIRTHDAY")
        @Temporal(TemporalType.DATE)
        public Date birthday;
        
        @Column(name = "INSERT_DATE")
        @Temporal(TemporalType.TIMESTAMP)
        public Date insertDate;

        @Column(name = "VERSION", length = 18)
        public String version;

        @Temporal(TemporalType.DATE)
        public Date getBirthday() {
            return birthday;
        }

        public void setBirthday(Date birthday) {
            this.birthday = birthday;
        }
        
        @Temporal(TemporalType.TIMESTAMP)
        public Date getInsertDate() {
            return insertDate;
        }

        public void setInsertDate(Date insertDate) {
            this.insertDate = insertDate;
        }

        @Id
        @Column(name = "USER_ID")
        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }

    /** USER_ADDRESSに対応したエンティティ */
    @Table(name = "USER_ADDRESS")
    @Entity
    public static class Address {

        @Id
        @Column(name = "ADDRESS_ID", length = 15)
        public Long id;

        @Id
        @Column(name = "ADDRESS_CODE", length = 1)
        public String code;

        @Column(name = "USER_ID", length = 15)
        public Long userId;

        @Column(name = "POST_NO", columnDefinition = "char(7)")
        public String postNo;

        @Column(name = "ADDRESS", length = 400)
        public String address;

        public Address() {
        }

        public Address(Long id, String code, Long userId, String postNo, String address) {
            this.address = address;
            this.code = code;
            this.id = id;
            this.postNo = postNo;
            this.userId = userId;
        }

        @Id
        @Column(name = "ADDRESS_ID", length = 15)
        @GeneratedValue(strategy = GenerationType.TABLE, generator = "SEQ")
        @TableGenerator(name = "SEQ", pkColumnValue = "ADDRESS_ID_SEQ")
        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        @Id
        @Column(name = "ADDRESS_CODE", columnDefinition = "char(1)")
        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        @Column(name = "USER_ID", length = 15)
        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        @Column(name = "POST_NO", columnDefinition = "char(7)")
        public String getPostNo() {
            return postNo;
        }

        public void setPostNo(String postNo) {
            this.postNo = postNo;
        }

        @Column(name = "ADDRESS", length = 400)
        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }
    
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
    
    public static Date getDate(String date) {
        try {
            return sdf.parse(date);
        } catch (ParseException e) {
            // NOP
        }
        return null;
    }
}
