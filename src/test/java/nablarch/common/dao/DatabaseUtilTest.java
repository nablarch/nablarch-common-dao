package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import nablarch.test.support.db.helper.TargetDb;
import org.hamcrest.collection.IsMapContaining;

import nablarch.common.dao.DaoTestHelper.Address;
import nablarch.common.dao.DaoTestHelper.Users;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrictExpectations;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * {@link DatabaseUtil}のテストクラス。
 */
@RunWith(DatabaseTestRunner.class)
public class DatabaseUtilTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /** テストで使用するデータベース接続 */
    private TransactionManagerConnection connection;

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(Users.class);
        VariousDbTestHelper.createTable(Address.class);
        VariousDbTestHelper.createTable(WithSchemaEntity.class);
    }

    @Before
    public void setUp() throws Exception {
        ConnectionFactory connectionFactory = repositoryResource.getComponent("connectionFactory");
        connection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        DbConnectionContext.setConnection(connection);
        repositoryResource.addComponent("databaseMetaDataExtractor", null);
    }

    @After
    public void tearDown() throws Exception {
        DbConnectionContext.removeConnection();
        try {
            connection.terminate();
        } catch (Exception ignored) {
        }
    }

    /**
     * 主キーの取得テスト：主キーが単一
     *
     * @throws Exception
     */
    @Test
    public void getPrimaryKey_single() throws Exception {
        Map<String, Short> primaryKeys = DatabaseUtil.getPrimaryKey("DAO_USERS");
        assertThat("主キーは1つ", primaryKeys.size(), is(1));
        assertThat(primaryKeys.get("USER_ID"), is(Short.valueOf("1")));
    }

    /**
     * SQL型の取得テスト：デフォルトスキーマ。
     * Oracle用。
     * @throws Exception
     */
    @TargetDb(include = TargetDb.Db.ORACLE)
    @Test
    public void getSqlType_defaultSchema_Oracle() throws Exception {
        Map<String, Integer> actual = DatabaseUtil.getSqlTypeMap(null, "DAO_USERS");
        assertThat(actual.get("USER_ID"), is(Types.DECIMAL));
        assertThat(actual.get("NAME"), is(Types.VARCHAR));
        assertThat(actual.get("BIRTHDAY"), is(Types.TIMESTAMP));
        assertThat(actual.get("INSERT_DATE"), is(Types.TIMESTAMP));
        assertThat(actual.get("VERSION"), is(Types.DECIMAL));
        assertThat(actual.get("ACTIVE"), is(Types.DECIMAL));
    }

    /**
     * SQL型の取得テスト：デフォルトスキーマ。
     * SQLServer用。
     * @throws Exception
     */
    @TargetDb(include = TargetDb.Db.SQL_SERVER)
    @Test
    public void getSqlType_defaultSchema_SqlServer() throws Exception {
        Map<String, Integer> actual = DatabaseUtil.getSqlTypeMap(null, "DAO_USERS");
        assertThat(actual.get("USER_ID"), is(Types.NUMERIC));
        assertThat(actual.get("NAME"), is(Types.VARCHAR));
        assertThat(actual.get("BIRTHDAY"), is(Types.TIMESTAMP));
        assertThat(actual.get("INSERT_DATE"), is(Types.TIMESTAMP));
        assertThat(actual.get("VERSION"), is(Types.NUMERIC));
        assertThat(actual.get("ACTIVE"), is(Types.BIT));
    }

    /**
     * SQL型の取得テスト：デフォルトスキーマ。
     * PostgreSQL用。
     * @throws Exception
     */
    @TargetDb(include = TargetDb.Db.POSTGRE_SQL)
    @Test
    public void getSqlType_defaultSchema_PostgreSQL() throws Exception {
        Map<String, Integer> actual = DatabaseUtil.getSqlTypeMap(null, "DAO_USERS");
        assertThat(actual.get("USER_ID"), is(Types.BIGINT));
        assertThat(actual.get("NAME"), is(Types.VARCHAR));
        assertThat(actual.get("BIRTHDAY"), is(Types.DATE));
        assertThat(actual.get("INSERT_DATE"), is(Types.TIMESTAMP));
        assertThat(actual.get("VERSION"), is(Types.BIGINT));
        assertThat(actual.get("ACTIVE"), is(Types.BIT));
    }

    /**
     * SQL型の取得テスト：デフォルトスキーマ。
     * H2用。
     * @throws Exception
     */
    @TargetDb(include = TargetDb.Db.H2)
    @Test
    public void getSqlType_defaultSchema_H2() throws Exception {
        Map<String, Integer> actual = DatabaseUtil.getSqlTypeMap(null, "DAO_USERS");
        assertThat(actual.get("USER_ID"), is(Types.BIGINT));
        assertThat(actual.get("NAME"), is(Types.VARCHAR));
        assertThat(actual.get("BIRTHDAY"), is(Types.DATE));
        assertThat(actual.get("INSERT_DATE"), is(Types.TIMESTAMP));
        assertThat(actual.get("VERSION"), is(Types.BIGINT));
        assertThat(actual.get("ACTIVE"), is(Types.BOOLEAN));
    }

    /**
     * SQL型の取得テスト：デフォルトスキーマ。
     * DB2用。
     * @throws Exception
     */
    @TargetDb(include = TargetDb.Db.DB2)
    @Test
    public void getSqlType_defaultSchema_DB2() throws Exception {
        Map<String, Integer> actual = DatabaseUtil.getSqlTypeMap(null, "DAO_USERS");
        assertThat(actual.get("USER_ID"), is(Types.BIGINT));
        assertThat(actual.get("NAME"), is(Types.VARCHAR));
        assertThat(actual.get("BIRTHDAY"), is(Types.DATE));
        assertThat(actual.get("INSERT_DATE"), is(Types.TIMESTAMP));
        assertThat(actual.get("VERSION"), is(Types.BIGINT));
        assertThat(actual.get("ACTIVE"), is(Types.SMALLINT));
    }

    /**
     * SQL型の取得テスト：スキーマを指定。
     * Oracle用。
     * @throws Exception
     */
    @TargetDb(include = TargetDb.Db.ORACLE)
    @Test
    public void getSqlType_otherSchema_Oracle() throws Exception {
        Map<String, Integer> actual = DatabaseUtil.getSqlTypeMap(null, "DAO_USERS");
        assertThat(actual.get("NAME"), is(Types.VARCHAR));
        actual = DatabaseUtil.getSqlTypeMap("test_schema", "TEST_SCHEMA_USERS");
        assertThat(actual.get("NAME"), is(Types.DECIMAL));
    }

    /**
     * SQL型の取得テスト：スキーマを指定。
     * SQLServer, PostgreSQL, H2, DB2用。
     * @throws Exception
     */
    @TargetDb(include = {TargetDb.Db.SQL_SERVER, TargetDb.Db.POSTGRE_SQL, TargetDb.Db.H2, TargetDb.Db.DB2} )
    @Test
    public void getSqlType_otherSchema_SqlServer_PostgreSQL_H2_DB2() throws Exception {
        Map<String, Integer> actual = DatabaseUtil.getSqlTypeMap(null, "DAO_USERS");
        assertThat(actual.get("NAME"), is(Types.VARCHAR));
        actual = DatabaseUtil.getSqlTypeMap("test_schema", "TEST_SCHEMA_USERS");
        assertThat(actual.get("NAME"), is(Types.INTEGER));
    }

    /**
     * SQL型の取得テスト：SQL型取得時にSQLエラーが発生した場合は、RuntimeExceptionが送出される。
     *
     * @throws Exception
     */
    @Test
    public void getSqlType_SQLException() throws Exception {
        new Expectations(DbConnectionContext.class) {{
            DbConnectionContext.getConnection();
            result = new SQLException("sql error");
        }};

        RuntimeException exception = null;
        try {
            DatabaseUtil.getSqlTypeMap(null, "DAO_USERS");
        } catch (RuntimeException e) {
            exception = e;
        }
        assertThat(exception.getCause(), is(instanceOf(SQLException.class)));
    }

    /**
     * カスタム実装での主キー取得テスト
     */
    @Test
    public void getPrimaryKey_customExtractor() throws Exception {
        repositoryResource.addComponent("databaseMetaDataExtractor", new DatabaseMetaDataExtractor() {
            @Override
            public Map<String, Short> getPrimaryKeys(String tableName) {
                if (!tableName.equals("DAO_USERS")) {
                    throw new IllegalArgumentException("table name was invalid");
                }
                return new HashMap<String, Short>() {{
                    put("key1", (short) 1);
                    put("key2", (short) 2);
                }};
            }
        });
        Map<String, Short> primaryKeys = DatabaseUtil.getPrimaryKey("DAO_USERS");
        assertThat("主キーは2つ", primaryKeys.size(), is(2));
        assertThat("key1", primaryKeys, IsMapContaining.hasEntry("key1", (short) 1));
        assertThat("key2", primaryKeys, IsMapContaining.hasEntry("key2", (short) 2));
    }

    /**
     * 主キーの取得テスト：主キーが複数
     *
     * @throws Exception
     */
    @Test
    public void getPrimaryKey_multi() throws Exception {
        Map<String, Short> primaryKeys = DatabaseUtil.getPrimaryKey("USER_ADDRESS");

        assertThat("主キーは2つ", primaryKeys.size(), is(2));
        assertThat(primaryKeys.get("ADDRESS_ID"), is(Short.valueOf("1")));
        assertThat(primaryKeys.get("ADDRESS_CODE"), is(Short.valueOf("2")));
    }

    /**
     * 主キーの取得テスト：主キー取得時にSQLエラーが発生した場合は、RuntimeExceptionが送出される。
     *
     * @throws Exception
     */
    @Test
    public void getPrimaryKey_SQLException() throws Exception {
        new Expectations(DbConnectionContext.class) {{
            DbConnectionContext.getConnection();
            result = new SQLException("sql error");
        }};

        RuntimeException exception = null;
        try {
            Map<String, Short> primaryKeys = DatabaseUtil.getPrimaryKey("USER_ADDRESS");
        } catch (RuntimeException e) {
            exception = e;
        }
        assertThat(exception.getCause(), is(instanceOf(SQLException.class)));
    }

    /**
     * 主キー取得のテスト：メタデータの取得に失敗するケース。
     *
     * @throws Exception
     */
    @Test(expected = IllegalStateException.class)
    public void getPrimaryKey_MetaDataError(@Mocked final AppDbConnection con) throws Exception {
        new Expectations(DbConnectionContext.class) {{
            DbConnectionContext.getConnection();
            result = con;
        }};

        DatabaseUtil.getPrimaryKey("USER_ADDRESS");
    }

    /**
     * 識別子の変換のテスト：大文字小文字を区別しない場合は、変換されないこと。
     *
     * @throws Exception
     */
    @Test
    public void convertIdentifiers_notConverted() throws Exception {

        new NonStrictExpectations(connection) {{
            final Connection connection = DatabaseUtilTest.this.connection.getConnection();
            final DatabaseMetaData metaData = connection.getMetaData();
            metaData.storesMixedCaseIdentifiers();
            result = true;
        }};

        String actual = DatabaseUtil.convertIdentifiers("Hoge_Fuga");
        assertThat("変換されないこと", actual, is("Hoge_Fuga"));
    }

    /**
     * 識別子の変換のテスト：大文字小文字を区別して大文字格納の場合は、大文字に変換されること。
     *
     * @throws Exception
     */
    @Test
    public void convertIdentifiers_convertUpper() throws Exception {
        final DatabaseMetaData metaData = connection.getConnection()
                .getMetaData();

        new NonStrictExpectations(metaData) {{
            metaData.storesMixedCaseIdentifiers();
            result = false;
            metaData.storesUpperCaseIdentifiers();
            result = true;
            metaData.storesLowerCaseIdentifiers();
            result = false;
        }};

        String actual = DatabaseUtil.convertIdentifiers("Hoge_Fuga");
        assertThat(actual, is("HOGE_FUGA"));
    }

    /**
     * 識別子の変換のテスト：大文字小文字を区別して小文字格納の場合は、小文字に変換されること。
     *
     * @throws Exception
     */
    @Test
    public void convertIdentifiers_convertLower() throws Exception {
        new NonStrictExpectations(connection) {{
            final Connection connection = DatabaseUtilTest.this.connection.getConnection();
            final DatabaseMetaData metaData = connection.getMetaData();
            metaData.storesMixedCaseIdentifiers();
            result = false;
            metaData.storesUpperCaseIdentifiers();
            result = false;
            metaData.storesLowerCaseIdentifiers();
            result = true;
        }};

        String actual = DatabaseUtil.convertIdentifiers("Hoge_Fuga");
        assertThat(actual, is("hoge_fuga"));
    }

    /**
     * 識別子の変換のテスト：大文字小文字を区別して格納されるはずだが、大文字小文字の判断ができない場合（通常はありえないはず）は変換されないこと
     *
     * @throws Exception
     */
    @Test
    public void convertIdentifiers_convertOther() throws Exception {

        new NonStrictExpectations(connection) {{
            final Connection connection = DatabaseUtilTest.this.connection.getConnection();
            final DatabaseMetaData metaData = connection.getMetaData();
            metaData.storesMixedCaseIdentifiers();
            result = false;
            metaData.storesUpperCaseIdentifiers();
            result = false;
            metaData.storesLowerCaseIdentifiers();
            result = false;
        }};

        String actual = DatabaseUtil.convertIdentifiers("Hoge_Fuga");
        assertThat(actual, is("Hoge_Fuga"));
    }

    /**
     * 識別子の変換のテスト：識別子の格納方法判断時にSQLExceptionが発生した場合
     *
     * @throws Exception
     */
    @Test
    public void convertIdentifiers_SQLException() throws Exception {

        new NonStrictExpectations(connection) {{
            final Connection connection = DatabaseUtilTest.this.connection.getConnection();
            final DatabaseMetaData metaData = connection.getMetaData();
            metaData.storesMixedCaseIdentifiers();
            result = new SQLException("error");
        }};

        RuntimeException exception = null;
        try {
            DatabaseUtil.convertIdentifiers("hoge_fuga");
        } catch (RuntimeException e) {
            exception = e;
        }

        assertThat(exception.getCause(), is(instanceOf(SQLException.class)));
    }

    /**
     * {@link DatabaseUtil#convertIdentifiers(DatabaseMetaData, String)}のテスト。
     * 機能的なテストは{@link DatabaseUtil#convertIdentifiers(String)}のテストで充足しているので、
     * ここでは疎通確認のため、無変換のケースのみ行う。
     *
     * @throws SQLException
     */
    @Test
    public void convertIdentifiersMetaData() throws SQLException {

        final DatabaseMetaData metaData = connection.getConnection()
                .getMetaData();
        new Expectations(metaData) {{
            metaData.storesMixedCaseIdentifiers();
            result = true;
        }};
        String actual = DatabaseUtil.convertIdentifiers(metaData, "Hoge_Fuga");
        assertThat("変換されないこと", actual, is("Hoge_Fuga"));
    }


    /**
     * 識別子の変換のテスト：識別子の格納方法判断時にSQLExceptionが発生した場合
     *
     * @throws SQLException
     */
    @Test
    public void convertIdentifiersMetaData_SQLException() throws SQLException {

        new NonStrictExpectations(connection) {{
            final Connection connection = DatabaseUtilTest.this.connection.getConnection();
            final DatabaseMetaData metaData = connection.getMetaData();
            metaData.storesMixedCaseIdentifiers();
            result = new SQLException("error");
        }};

        try {
            final Connection connection = DatabaseUtilTest.this.connection.getConnection();
            final DatabaseMetaData metaData = connection.getMetaData();
            DatabaseUtil.convertIdentifiers(metaData, "hoge_fuga");
            fail();
        } catch (RuntimeException e) {
            assertThat(e.getCause(), is(instanceOf(SQLException.class)));
        }
    }

    // ---------------------------------------- test entity

    @Entity
    @Table(name = "TEST_SCHEMA_USERS", schema = "test_schema")
    public static class WithSchemaEntity {

        @Id
        @Column(name = "USER_ID", length = 15)
        public Long id;

        @Column(name = "NAME", length = 1)
        public Integer name;
    }
}
