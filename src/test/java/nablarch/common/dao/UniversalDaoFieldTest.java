package nablarch.common.dao;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import java.util.Calendar;
import java.util.Date;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author Ryota Yoshinouchi
 */
@RunWith(DatabaseTestRunner.class)
public class UniversalDaoFieldTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Entity
    @Table(name = "USERS")
    @Access(AccessType.FIELD)
    public static class Users {

        @Id
        @Column(name = "USER_ID", length = 15)
        public Long id;

        @Column(name = "NAME", length = 100)
        public String name;

        @Column(name = "BIRTHDAY")
        @Temporal(TemporalType.DATE)
        public Date birthday;

        public Users() {
        }

        public Users(Long id) {
            this.id = id;
        }

        public Users(Long id, String name, Date birthday) {
            this.id = id;
            this.name = name;
            this.birthday = birthday;
        }

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

        public Date getBirthday() {
            return birthday;
        }

        public void setBirthday(Date birthday) {
            this.birthday = birthday;
        }
    }

    @Entity
    @Table(name = "ILLEGAL_FIELD_USERS")
    @Access(AccessType.FIELD)
    public static class IllegalFieldUsers {

        public String userName;

        public String getName() {
            return userName;
        }

        public void setName(String name) {
            userName = name;
        }
    }

    @Entity
    @Table(name = "ILLEGAL_PROPERTY_USERS")
    public static class IllegalPropertyUsers {

        public String name;

        public void setName(String name) {
            this.name = name;
        }
    }

    @Before
    public void setUp() throws Exception {
        ConnectionFactory connectionFactory = repositoryResource.getComponent("connectionFactory");
        TransactionManagerConnection connection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        DbConnectionContext.setConnection(connection);
        VariousDbTestHelper.createTable(Users.class);
    }

    @After
    public void tearDown() throws Exception {
        final TransactionManagerConnection connection = DbConnectionContext.getTransactionManagerConnection();
        try {
            connection.terminate();
        } catch (Exception ignored) {
        } finally {
            DbConnectionContext.removeConnection();
        }
    }

    @Test
    public void フィールドの情報を元にインサートできる事() {
        final Users user = new Users();
        user.setId(1L);
        user.setName("name");
        user.setBirthday(new Date());

        UniversalDao.insert(user);

        DbConnectionContext.getTransactionManagerConnection().commit();

        final Users actual = VariousDbTestHelper.findById(Users.class, 1L);

        assertThat(actual.getId(), is(user.getId()));
        assertThat(actual.getName(), is(user.getName()));
        assertThat(actual.getBirthday(), is(trimTime(user.getBirthday())));
    }

    @Test
    public void フィールドの情報を元にアップデートできる事() {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", new Date()),
                new Users(2L, "name_2", new Date()),
                new Users(3L, "name_3", new Date())
        );

        final Users user = UniversalDao.findById(Users.class, 2L);
        user.setName("なまえに更新");

        int updateCount = UniversalDao.update(user);

        assertThat("更新されたレコードは1", updateCount, is(1));

        Users actual = UniversalDao.findById(Users.class, 2L);
        assertThat(actual.getId(), is(user.getId()));
        assertThat(actual.getName(), is(user.getName()));
        assertThat(actual.getBirthday(), is(user.getBirthday()));
    }

    @Test
    public void フィールドの情報を元に削除できる事() {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 10; i++) {
            VariousDbTestHelper.insert(new Users((long) (i + 1)));
        }

        assertThat("削除対象が存在していること", UniversalDao.findById(Users.class, 3L), notNullValue());
        UniversalDao.delete(new Users(3L));

        expectedException.expect(NoDataException.class);
        UniversalDao.findById(Users.class, 3L);
    }

    @Test
    public void フィールドの情報を元にIDを指定して取得できる事() {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", new Date()),
                new Users(2L, "name_2", new Date()),
                new Users(3L, "name_3", new Date())
        );

        Users user = UniversalDao.findById(Users.class, 2L);
        assertThat(user.getId(), is(2L));
        assertThat(user.getName(), is("name_2"));
        assertThat(user.getBirthday(), is(trimTime(new Date())));
    }

    @Test
    public void プロパティ名に対応するフィールドが無い場合に例外が送出されること() {
        expectedException.expect(IllegalArgumentException.class);
        UniversalDao.findById(IllegalFieldUsers.class, 3L);
    }

    @Test
    public void フィールドに対応するgetterが無い場合に例外が送出されること() {
        expectedException.expect(IllegalArgumentException.class);
        UniversalDao.findById(IllegalPropertyUsers.class, 3L);
    }

    private Date trimTime(Date date) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }
}
