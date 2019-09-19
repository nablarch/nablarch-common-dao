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
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(DatabaseTestRunner.class)
public class NCharTest {
    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /** テスト用データベース接続 */
    private TransactionManagerConnection connection;

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(NCharTable.class);
    }

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

    @Test
    public void ShiftJISにマッピングされない文字のテスト() {

        NCharTable table = new NCharTable();
        table.id = 1L;
        table.ncharColumn = "槗桺婷琨刘吴翟";
        table.nvarcharColumn = "槗桺婷琨刘吴翟";
        VariousDbTestHelper.delete(NCharTable.class);
        VariousDbTestHelper.setUpTable(table);

        NCharTable entity = UniversalDao.findById(NCharTable.class, 1L);
        assertThat(entity.ncharColumn, is("槗桺婷琨刘吴翟"));
        assertThat(entity.nvarcharColumn, is("槗桺婷琨刘吴翟"));
    }

    @Entity
    @Table(name = "nchar_table")
    public static class NCharTable {

        @Id
        @Column(name = "id", length = 18)
        public Long id;

        @Column(name = "nchar_column", columnDefinition = "NCHAR(10)")
        public String ncharColumn;

        @Column(name = "nvarchar_column", columnDefinition = "NVARCHAR(10)")
        public String nvarcharColumn;

        @Id
        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getNcharColumn() {
            return ncharColumn;
        }

        public void setNcharColumn(String ncharColumn) {
            this.ncharColumn = ncharColumn;
        }

        public String getNvarcharColumn() {
            return nvarcharColumn;
        }

        public void setNvarcharColumn(String nvarcharColumn) {
            this.nvarcharColumn = nvarcharColumn;
        }
    }
}
