package nablarch.common.dao;

import jakarta.persistence.GenerationType;
import nablarch.common.idgenerator.IdGenerator;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.test.support.reflection.ReflectionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link BasicDaoContextFactory}のテストクラス。
 */
public class BasicDaoContextFactoryTest {

    /** テスト対象 */
    BasicDaoContextFactory sut = new BasicDaoContextFactory();

    private final IdGenerator mockTableIdGenerator = mock(IdGenerator.class);

    private final IdGenerator mockSequenceIdGenerator = mock(IdGenerator.class);

    private final TransactionManagerConnection mockConnection = mock(TransactionManagerConnection.class);

    @Before
    public void setUp() throws Exception {
        DbConnectionContext.removeConnection();
        sut.setDbConnection(mockConnection);
    }

    @After
    public void tearDown() throws Exception {
        DbConnectionContext.removeConnection();
    }

    /**
     * シーケンスとテーブルの両方の採番が設定されているケース
     *
     * @throws Exception
     */
    @Test
    public void create_TableAndSequenceGen() throws Exception {
        sut.setTableIdGenerator(mockTableIdGenerator);
        sut.setSequenceIdGenerator(mockSequenceIdGenerator);

        BasicDaoContext context = (BasicDaoContext) sut.create();
        final Map<GenerationType, IdGenerator> generators = ReflectionUtil.getFieldValue(context, "idGenerators");
        assertThat("シーケンス採番が設定されていること",
                generators.get(GenerationType.SEQUENCE),
                is(sameInstance(mockSequenceIdGenerator)));

        assertThat("テーブル採番が設定されていること",
                generators.get(GenerationType.TABLE),
                is(sameInstance(mockTableIdGenerator)));
    }

    /**
     * Dialectの取得先の確認。
     * <p />
     * {@link TransactionManagerConnection#getDialect()}で取得できるインスタンスが参照されること。
     *
     * @throws Exception
     */
    @Test
    public void create_Dialect() throws Exception {
        final DefaultDialect inputDialect = new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return true;
            }
        };
        when(mockConnection.getDialect()).thenReturn(inputDialect);
        final BasicDaoContext context = (BasicDaoContext) sut.create();

        final DefaultDialect dialect = ReflectionUtil.getFieldValue(context, "dialect");
        assertThat(dialect, is(sameInstance(inputDialect)));
        assertThat(dialect.supportsIdentity(), is(true));
    }

    /**
     * SQLビルダーを設定していないケース。
     * <p/>
     * デフォルトの{@link StandardSqlBuilder}が使用されること。
     *
     * @throws Exception
     */
    @Test
    public void create_notSqlBuilder() throws Exception {
        final BasicDaoContext context = (BasicDaoContext) sut.create();

        final Object builder = ReflectionUtil.getFieldValue(context, "sqlBuilder");
        assertThat(builder, is(instanceOf(StandardSqlBuilder.class)));
    }

    /**
     * SQLビルダーを設定しているケース。
     * <p/>
     * そのSQLビルダーが使用されること。
     *
     * @throws Exception
     */
    @Test
    public void create_SqlBuilder() throws Exception {
        final String SQL = "主キー検索SQL";
        StandardSqlBuilder builder = new StandardSqlBuilder() {
            @Override
            public <T> String buildSelectByIdSql(Class<T> entityClass) {
                return SQL;
            }
        };

        sut.setSqlBuilder(builder);
        final BasicDaoContext context = (BasicDaoContext) sut.create();

        final StandardSqlBuilder result = ReflectionUtil.getFieldValue(context, "sqlBuilder");

        assertThat(result, is(sameInstance(builder)));
        assertThat(result.buildSelectByIdSql(this.getClass()), is(SQL));
    }

    /**
     * ファクトリクラスにDB接続が設定されていないケース。
     * <p/>
     * データベース接続は、{@link nablarch.core.db.connection.DbConnectionContext}から取得されること。
     *
     * @throws Exception
     */
    @Test
    public void create_NotDbConnection() throws Exception {
        TransactionManagerConnection mock = mock(TransactionManagerConnection.class);
        
        // Factoryにコネクションは設定されていない。
        sut.setDbConnection(null);

        DbConnectionContext.setConnection(mock);

        BasicDaoContext context = (BasicDaoContext) sut.create();

        final TransactionManagerConnection result = ReflectionUtil.getFieldValue(context, "dbConnection");
        assertThat("DbConnectionContext上のDB接続が設定されていること", result, is(mock));
    }

    /**
     * ファクトリクラスにDB接続が設定されているケース。
     * <p/>
     * データベース接続は、DbConnectionContextよりもファクトリクラス側のものが優先されること。
     *
     * @throws Exception
     */
    @Test
    public void create_HasDbConnection() throws Exception {
        TransactionManagerConnection mock = mock(TransactionManagerConnection.class);
        
        DbConnectionContext.setConnection(mock);

        BasicDaoContext context = (BasicDaoContext) sut.create();

        final TransactionManagerConnection connection = ReflectionUtil.getFieldValue(context, "dbConnection");
        assertThat("ファクトリ側のDB接続が設定されていること", connection, is(mockConnection));
    }
}

