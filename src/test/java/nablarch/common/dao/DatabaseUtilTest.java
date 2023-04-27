package nablarch.common.dao;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
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
import org.hamcrest.collection.IsMapContaining;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.text.IsEqualIgnoringCase.equalToIgnoringCase;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
        TransactionManagerConnection rawConnection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        this.connection = spy(rawConnection);
        DbConnectionContext.setConnection(this.connection);
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
        Connection connection = mock(Connection.class);
        when(this.connection.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenThrow(new SQLException("sql error"));
        
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
    public void getPrimaryKey_MetaDataError() throws Exception {
        AppDbConnection con = mock(AppDbConnection.class);
        try (final MockedStatic<DbConnectionContext> mocked = mockStatic(DbConnectionContext.class)) {
            mocked.when(DbConnectionContext::getConnection).thenReturn(con);
            DatabaseUtil.getPrimaryKey("USER_ADDRESS");
        }
    }

    /**
     * 識別子の変換のテスト：大文字小文字を区別しない場合は、変換されないこと。
     *
     * @throws Exception
     */
    @Test
    public void convertIdentifiers_notConverted() throws Exception {

        Connection connection = mock(Connection.class, RETURNS_DEEP_STUBS);
        when(this.connection.getConnection()).thenReturn(connection);
        when(connection.getMetaData().storesMixedCaseIdentifiers()).thenReturn(true);

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
        final DatabaseMetaData metaData = spy(DatabaseUtil.getMetaData());
        
        when(metaData.storesMixedCaseIdentifiers()).thenReturn(false);
        when(metaData.storesUpperCaseIdentifiers()).thenReturn(true);
        when(metaData.storesLowerCaseIdentifiers()).thenReturn(false);

        String actual = DatabaseUtil.convertIdentifiers("Hoge_Fuga");
        assertThat(actual, equalToIgnoringCase("HOGE_FUGA"));
    }

    /**
     * 識別子の変換のテスト：大文字小文字を区別して小文字格納の場合は、小文字に変換されること。
     *
     * @throws Exception
     */
    @Test
    public void convertIdentifiers_convertLower() throws Exception {
        Connection connection = mock(Connection.class);
        when(this.connection.getConnection()).thenReturn(connection);

        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(metaData);
        
        when(metaData.storesMixedCaseIdentifiers()).thenReturn(false);
        when(metaData.storesUpperCaseIdentifiers()).thenReturn(false);
        when(metaData.storesLowerCaseIdentifiers()).thenReturn(true);
        
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

        Connection connection = mock(Connection.class);
        when(this.connection.getConnection()).thenReturn(connection);
        
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(metaData);
        
        when(metaData.storesMixedCaseIdentifiers()).thenReturn(false);
        when(metaData.storesUpperCaseIdentifiers()).thenReturn(false);
        when(metaData.storesLowerCaseIdentifiers()).thenReturn(false);

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

        Connection connection = mock(Connection.class);
        when(this.connection.getConnection()).thenReturn(connection);

        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(metaData);
        
        when(metaData.storesMixedCaseIdentifiers()).thenThrow(new SQLException("error"));

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

        final DatabaseMetaData metaData = spy(connection.getConnection().getMetaData());
        when(metaData.storesMixedCaseIdentifiers()).thenReturn(true);
        
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

        final DatabaseMetaData metaData = spy(connection.getConnection().getMetaData());
        when(metaData.storesMixedCaseIdentifiers()).thenThrow(new SQLException("error"));

        try {
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
