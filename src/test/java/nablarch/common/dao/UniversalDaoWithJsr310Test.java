package nablarch.common.dao;

import nablarch.common.dao.entity.Jsr310Column;
import nablarch.common.dao.entity.Jsr310ColumnForSqlServer;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.TargetDb.Db;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * {{@link UniversalDao}のテストクラス。
 * <p> 
 * Date and Time API（JSR-310）型の登録、検索の動作確認テストを実施する。
 * Entityクラスの日時型の定義方法がSQLServerとそれ以外のDBで異なるため、分けてテストを実施する。
 * 
 */
@RunWith(DatabaseTestRunner.class)
public class UniversalDaoWithJsr310Test {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("universal-dao-with-jsr310-test.xml");

    /** テスト用データベース接続 */
    private TransactionManagerConnection connection;

    @Before
    public void setUp() {
        ConnectionFactory connectionFactory = repositoryResource.getComponent("connectionFactory");
        connection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        DbConnectionContext.setConnection(connection);
    }

    @After
    public void tearDown() {
        DbConnectionContext.removeConnection();
        try {
            connection.terminate();
        } catch (Exception ignored) {
        }
    }

    /**
     * Date and Time API（JSR-310）型（{@link LocalDate}, {@link LocalDateTime}）のフィールドを持つEntityのデータを登録できること（SQLServer以外）。
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void test_insertJsr310Column() {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        final Jsr310Column entity = new Jsr310Column();
        entity.id = 1L;
        entity.localDate = LocalDate.parse("2015-04-01");
        entity.localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        UniversalDao.insert(entity);
        connection.commit();

        final Jsr310Column actual = VariousDbTestHelper.findById(Jsr310Column.class, entity.id);
        assertThat(actual.localDate, is(entity.localDate));
        assertThat(actual.localDateTime, is(entity.localDateTime));
    }

    /**
     * Date and Time API（JSR-310）型（{@link LocalDate}, {@link LocalDateTime}）のフィールドを持つEntityのデータを登録できること（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void test_insertJsr310Column_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);
        final Jsr310ColumnForSqlServer entity = new Jsr310ColumnForSqlServer();
        entity.id = 1L;
        entity.localDate = LocalDate.parse("2015-04-01");
        entity.localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        UniversalDao.insert(entity);
        connection.commit();

        final Jsr310ColumnForSqlServer actual = VariousDbTestHelper.findById(Jsr310ColumnForSqlServer.class, entity.id);
        assertThat(actual.localDate, is(entity.localDate));
        assertThat(actual.localDateTime, is(entity.localDateTime));
    }
    
    /**
     * Date and Time API（JSR-310）型（{@link LocalDate}, {@link LocalDateTime}）のフィールドを持つEntityのデータを更新できること（SQLServer以外）。
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void test_updateJsr310Column() {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        final Jsr310Column entity = new Jsr310Column();
        entity.id = 12345L;
        entity.localDate = LocalDate.parse("2015-04-01");
        entity.localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        VariousDbTestHelper.insert(entity);

        entity.localDate = LocalDate.parse("2014-03-31");
        entity.localDateTime =  LocalDateTime.parse("2014-03-31T01:23:45");
        UniversalDao.update(entity);
        connection.commit();

        final Jsr310Column actual = VariousDbTestHelper.findById(Jsr310Column.class, entity.id);
        assertThat(actual.localDate, is(entity.localDate));
        assertThat(actual.localDateTime, is(entity.localDateTime));

    }

    /**
     * Date and Time API（JSR-310）型（{@link LocalDate}, {@link LocalDateTime}）のフィールドを持つEntityのデータを更新できること（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void test_updateJsr310Column_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);
        final Jsr310ColumnForSqlServer entity = new Jsr310ColumnForSqlServer();
        entity.id = 12345L;
        entity.localDate = LocalDate.parse("2015-04-01");
        entity.localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        VariousDbTestHelper.insert(entity);

        entity.localDate = LocalDate.parse("2014-03-31");
        entity.localDateTime =  LocalDateTime.parse("2014-03-31T01:23:45");
        UniversalDao.update(entity);
        connection.commit();

        final Jsr310ColumnForSqlServer actual = VariousDbTestHelper.findById(Jsr310ColumnForSqlServer.class, entity.id);
        assertThat(actual.localDate, is(entity.localDate));
        assertThat(actual.localDateTime, is(entity.localDateTime));

    }
    
    /**
     * {@link UniversalDao#findBySqlFile(Class, String, Object)}を使用して{@link LocalDate}でのデータ検索ができること（SQLServer以外）。
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void test_findAllBySqlFile_localDate() {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        final Jsr310Column entity = new Jsr310Column();
        entity.id = 12345L;
        entity.localDate = LocalDate.parse("2015-04-01");
        VariousDbTestHelper.insert(entity);

        // DBによってDateを保持する精度が異なるため、「指定したlocalDate以降であること」を検索条件とする
        final EntityList<Jsr310Column> actual = UniversalDao.findAllBySqlFile(Jsr310Column.class,
                "find_where_local_date_greater_than", new Object[] { entity.localDate.minusDays(1) });

        assertThat(actual.size(), is(1));
        assertThat(actual.get(0).localDate, is(entity.localDate));
    }

    /**
     * {@link UniversalDao#findBySqlFile(Class, String, Object)}を使用して{@link LocalDate}でのデータ検索ができること（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void test_findAllBySqlFile_localDate_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);
        final Jsr310ColumnForSqlServer entity = new Jsr310ColumnForSqlServer();
        entity.id = 12345L;
        entity.localDate = LocalDate.parse("2015-04-01");
        VariousDbTestHelper.insert(entity);

        // DBによってDateを保持する精度が異なるため、「指定したlocalDate以降であること」を検索条件とする
        final EntityList<Jsr310ColumnForSqlServer> actual = UniversalDao.findAllBySqlFile(Jsr310ColumnForSqlServer.class,
            "find_where_local_date_greater_than", new Object[] { entity.localDate.minusDays(1) });

        assertThat(actual.size(), is(1));
        assertThat(actual.get(0).localDate, is(entity.localDate));
    }
    
    /**
     * {@link UniversalDao#findBySqlFile(Class, String, Object)}を使用して{@link LocalDateTime}でのデータ検索ができること（SQLServer以外）。
     */
    @Test
    @TargetDb(exclude = Db.SQL_SERVER)
    public void test_findAllBySqlFile_localDateTime() {
        VariousDbTestHelper.createTable(Jsr310Column.class);
        final Jsr310Column entity = new Jsr310Column();
        entity.id = 12345L;
        entity.localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        VariousDbTestHelper.insert(entity);

        // DBによってTimestampを保持する精度が異なるため、「指定したlocalDateTime以降であること」を検索条件とする
        final EntityList<Jsr310Column> actual = UniversalDao.findAllBySqlFile(Jsr310Column.class,
                "find_where_local_date_time_greater_than", new Object[] { entity.localDateTime.minusMinutes(1) });

        assertThat(actual.size(), is(1));
        assertThat(actual.get(0).localDateTime, is(entity.localDateTime));
    }

    /**
     * {@link UniversalDao#findBySqlFile(Class, String, Object)}を使用して{@link LocalDateTime}でのデータ検索ができること（SQLServer用）。
     */
    @Test
    @TargetDb(include = Db.SQL_SERVER)
    public void test_findAllBySqlFile_localDateTime_SQLServer() {
        VariousDbTestHelper.createTable(Jsr310ColumnForSqlServer.class);
        final Jsr310ColumnForSqlServer entity = new Jsr310ColumnForSqlServer();
        entity.id = 12345L;
        entity.localDateTime =  LocalDateTime.parse("2015-04-01T12:34:56");
        VariousDbTestHelper.insert(entity);

        // DBによってTimestampを保持する精度が異なるため、「指定したlocalDateTime以降であること」を検索条件とする
        final EntityList<Jsr310ColumnForSqlServer> actual = UniversalDao.findAllBySqlFile(Jsr310ColumnForSqlServer.class,
            "find_where_local_date_time_greater_than", new Object[] { entity.localDateTime.minusMinutes(1) });

        assertThat(actual.size(), is(1));
        assertThat(actual.get(0).localDateTime, is(entity.localDateTime));
    }
}

