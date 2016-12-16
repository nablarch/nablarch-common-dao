package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link UniversalDao}の{@link Table#schema()}を指定した場合のテスト。
 */
@RunWith(DatabaseTestRunner.class)
public class UniversalDaoWithSchemaTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /** テスト用データベース接続 */
    private TransactionManagerConnection connection;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Connection connection = VariousDbTestHelper.getNativeConnection();
        connection.setAutoCommit(false);
        try {

            final PreparedStatement drop = connection.prepareStatement("drop table ssd_master.user_schema_table");
            try {
                drop.executeUpdate();
            } catch (SQLException ignore) {
                // nop
            }
            connection.commit();

            final PreparedStatement insert = connection.prepareStatement(
                    "create table ssd_master.user_schema_table ("
                            + " user_id integer not null,"
                            + " name varchar(100) not null,"
                            + " PRIMARY KEY (user_id))");
            insert.execute();
            insert.close();
            connection.commit();
        } finally {
            connection.close();
        }
    }

    @Before
    public void setUp() throws Exception {
        ConnectionFactory connectionFactory = repositoryResource.getComponent("connectionFactory");
        connection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        DbConnectionContext.setConnection(connection);

        final SqlPStatement statement = connection.prepareStatement("truncate table ssd_master.user_schema_table");
        statement.executeUpdate();
        connection.commit();
    }

    @After
    public void tearDown() throws Exception {
        DbConnectionContext.removeConnection();
        connection.terminate();
    }

    /**
     * {@link UniversalDao#findById(Class, Object...)}がスキーマ指定で実行できること
     */
    @Test
    public void testFindById() throws Exception {
        // -------------------------------------------------- setup data
        insert(1L, "name1");
        insert(2L, "name2");
        insert(3L, "name3");

        // -------------------------------------------------- execute
        final UsersWithSchemaEntity result = UniversalDao.findById(UsersWithSchemaEntity.class, 2L);

        // -------------------------------------------------- assert
        assertThat(result.getId(), is(2L));
        assertThat(result.getName(), is("name2"));
    }

    /**
     * {@link UniversalDao#findAll(Class)}がスキーマ指定で実行できること
     */
    @Test
    public void testFindAll() throws Exception {
        // -------------------------------------------------- setup data
        insert(1L, "hoge");
        insert(100L, "fuga");

        // -------------------------------------------------- execute
        final EntityList<UsersWithSchemaEntity> result = UniversalDao.findAll(UsersWithSchemaEntity.class);

        // -------------------------------------------------- assert
        assertThat("2件取得できること", result.size(), is(2));
    }

    /**
     * {@link UniversalDao#delete(Object)}がスキーマ指定で実行できること
     */
    @Test
    public void testDelete() throws Exception {
        // -------------------------------------------------- setup data
        insert(1L, "1");
        insert(2L, "2");
        insert(3L, "3");
        insert(4L, "4");

        // -------------------------------------------------- execute
        final UsersWithSchemaEntity entity = new UsersWithSchemaEntity();
        entity.setId(3L);
        final int deleted = UniversalDao.delete(entity);
        connection.commit();

        // -------------------------------------------------- assert
        assertThat("1レコード削除される", deleted, is(1));

        final EntityList<UsersWithSchemaEntity> result = UniversalDao.findAll(UsersWithSchemaEntity.class);
        assertThat("1レコード削除され3レコードになる", result.size(), is(3));
    }

    /**
     * {@link UniversalDao#insert(Object)}がスキーマ指定で実行できること
     */
    @Test
    public void testInsert() throws Exception {
        final EntityList<UsersWithSchemaEntity> initial = UniversalDao.findAll(UsersWithSchemaEntity.class);
        assertThat("初期データは存在しない", initial.size(), is(0));

        // -------------------------------------------------- execute
        final UsersWithSchemaEntity entity = new UsersWithSchemaEntity();
        entity.setId(2L);
        entity.setName("name");
        UniversalDao.insert(entity);
        connection.commit();

        // -------------------------------------------------- assert
        final UsersWithSchemaEntity result = UniversalDao.findById(UsersWithSchemaEntity.class, 2L);

        assertThat(result.getId(), is(2L));
        assertThat(result.getName(), is("name"));

    }

    /**
     * {@link UniversalDao#update(Object)}がスキーマ指定で実行できること
     */
    @Test
    public void testUpdate() throws Exception {
        // -------------------------------------------------- setup data
        insert(10L, "name");

        // -------------------------------------------------- execute
        final UsersWithSchemaEntity entity = new UsersWithSchemaEntity();
        entity.setId(10L);
        entity.setName("更新");
        final int updated = UniversalDao.update(entity);

        // -------------------------------------------------- assert
        assertThat("1レコード更新されること", updated, is(1));
        connection.commit();

        final UsersWithSchemaEntity result = UniversalDao.findById(UsersWithSchemaEntity.class, 10L);
        assertThat(result.getName(), is("更新"));
    }

    /**
     * テスト用のデータをセットアップ(insert)する。
     * @param id idカラムの値
     * @param name nameカラムの値
     */
    private void insert(Long id, String name) {
        final SqlPStatement statement = connection.prepareStatement(
                "insert into ssd_master.user_schema_table values (?, ?)");
        statement.setLong(1, id);
        statement.setString(2, name);
        statement.executeUpdate();
        connection.commit();
    }

    @Entity
    @Table(name = "user_schema_table", schema = "ssd_master")
    public static class UsersWithSchemaEntity {

        private Long id;

        private String name;

        @Id
        @Column(name = "user_id")
        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        @Column(name = "name")
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}

