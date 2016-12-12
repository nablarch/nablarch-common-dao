package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import javax.persistence.Id;
import javax.persistence.Version;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.Types;
import java.util.List;

@RunWith(DatabaseTestRunner.class)
public class ColumnMetaTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /** テストで使用するデータベース接続 */
    private TransactionManagerConnection connection;

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(DaoTestHelper.Users.class);
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
     * {@link ColumnMeta#getSqlType()}のテスト。
     * @throws Exception
     */
    @Test
    public void getSqlType() throws Exception {
        // データベースからSQL型が取得できれば、ColumnMetaからも取得できる。
        List<ColumnMeta> idColumns = EntityUtil.findIdColumns(DaoTestHelper.Users.class);
        int sqlType = idColumns.get(0).getSqlType();
        assertThat(sqlType, is(Types.DECIMAL));

        // データベースからSQL型が取得できない場合、ColumnMetaから取得できない
        idColumns = EntityUtil.findIdColumns(Entity.class);
        try {
            idColumns.get(0).getSqlType();
            fail();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("SQL type is not found."));
        }

    }

    /**
     * {@link ColumnMeta#equals(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void equals() throws Exception {
        ColumnMeta entityId = EntityUtil.findIdColumns(Entity.class)
                .get(0);

        ColumnMeta entity2Id = EntityUtil.findIdColumns(Entity2.class)
                .get(0);

        EntityUtil.clearCache();

        assertThat("同一Entityの同一カラムの結果はtrue",
                entityId.equals(EntityUtil.findIdColumns(Entity.class)
                        .get(0)), is(true));

        assertThat("同一エンティティの異なるカラムはfalse", entityId.equals(EntityUtil.findVersionColumn(new Entity())), is(false));

        assertThat("カラム名は同じでもEntityが異なるのでfalse", entityId.equals(entity2Id), is(false));

        assertThat("nullとの比較はfalse", entityId.equals(null), is(false));
        assertThat("異なるオブジェクトとの比較はfalse", entityId.equals(""), is(false));
    }

    public static class Entity {

        @Id
        public Long getId() {
            return 0L;
        }

        public String getName() {
            return "";
        }

        @Version
        public Long getVersion() {
            return 0L;
        }
    }

    public static class Entity2 {

        @Id
        public Long getId() {
            return 0L;
        }

        public String getName() {
            return "";
        }
    }
}