package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.util.Iterator;

import org.hamcrest.CoreMatchers;

import nablarch.common.dao.DaoTestHelper.Users;
import nablarch.core.db.DbAccessException;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.ResultSetIterator;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.DateUtil;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 * {@link DeferredEntityList}のテストクラス。
 */
@RunWith(DatabaseTestRunner.class)
public class DeferredEntityListTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /** データベース接続 */
    private TransactionManagerConnection connection;
    
    @BeforeClass
    public static void setUpClass() {
        VariousDbTestHelper.createTable(DaoTestHelper.Users.class);
    }

    @Before
    public void setUp() throws Exception {
        ConnectionFactory connectionFactory = repositoryResource.getComponent("connectionFactory");
        connection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
    }

    @After
    public void tearDownClass() {
        connection.terminate();
    }

    /**
     * {@link DeferredEntityList#iterator()}で検索結果を取得できること。
     */
    @Test
    public void iterator() throws Exception {
        DeferredEntityList<SqlRow> sut = createDeferredEntity();

        Iterator<SqlRow> iterator = sut.iterator();

        assertThat("次のレコードがあること", iterator.hasNext(), is(true));
        assertThat(iterator.next()
                .getInteger("userId"), is(1));
        assertThat("次のレコードがあること", iterator.hasNext(), is(true));
        assertThat(iterator.next()
                .getInteger("userId"), is(2));
        assertThat("次のレコードがあること", iterator.hasNext(), is(true));
        assertThat(iterator.next()
                .getInteger("userId"), is(3));
        assertThat("次のレコードはないこと", iterator.hasNext(), is(false));
        try {
            iterator.remove();
            fail("ここはとおらない。");
        } catch (Exception e) {
            assertThat(e, CoreMatchers.instanceOf(UnsupportedOperationException.class));
        }

    }

    /**
     * {@link DeferredEntityList#iterator()}で取得した{@link java.util.Iterator#remove()}はサポートされていないこと。
     *
     * @throws Exception
     */
    @Test(expected = UnsupportedOperationException.class)
    public void iterator_remove() throws Exception {
        DeferredEntityList<SqlRow> sut = createDeferredEntity();

        Iterator<SqlRow> iterator = sut.iterator();
        assertThat(iterator.hasNext(), is(true));
        iterator.remove();
    }

    /**
     * {@link DeferredEntityList#close()}のテスト。
     */
    @Test(expected = DbAccessException.class)
    public void close() throws Exception {
        DeferredEntityList<SqlRow> sut = createDeferredEntity();

        Iterator<SqlRow> iterator = sut.iterator();
        assertThat("レコードがあることを確認", iterator.hasNext(), is(true));
        sut.close();

        // 接続がクローズされているのでここで例外が発生すること
        iterator.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void listIterator() throws Exception {
        createDeferredEntity().listIterator();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void listIterator_int() throws Exception {
        createDeferredEntity().listIterator(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void add() throws Exception {
        createDeferredEntity().add(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addAll() throws Exception {
        createDeferredEntity().addAll(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void clear() throws Exception {
        createDeferredEntity().clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void contains() throws Exception {
        createDeferredEntity().contains(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void ensureCapacity() throws Exception {
        createDeferredEntity().ensureCapacity(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void get() throws Exception {
        createDeferredEntity().get(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void indexOf() throws Exception {
        createDeferredEntity().indexOf(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void isEmpty() throws Exception {
        createDeferredEntity().isEmpty();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void lastIndexOf() throws Exception {
        createDeferredEntity().lastIndexOf(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove() throws Exception {
        createDeferredEntity().remove(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeRange() throws Exception {
        createDeferredEntity().removeRange(0, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void size() throws Exception {
        createDeferredEntity().size();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void toArray() throws Exception {
        createDeferredEntity().toArray();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void toArray_t() throws Exception {
        createDeferredEntity().toArray(new Object[0]);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void trimToSize() throws Exception {
        createDeferredEntity().trimToSize();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void sublist() throws Exception {
        createDeferredEntity().subList(0, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void containsAll() throws Exception {
        createDeferredEntity().containsAll(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeAll() throws Exception {
        createDeferredEntity().removeAll(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void retainAll() throws Exception {
        createDeferredEntity().retainAll(null);
    }

    @Test
    public void testToString() throws Exception {
        assertThat(createDeferredEntity().toString(), is("DeferredEntityList"));
    }

    /**
     * テスト対象の{@link DeferredEntityList}を生成する。
     */
    private DeferredEntityList<SqlRow> createDeferredEntity() {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20140101"), DaoTestHelper.getDate("20150401123456")),
                new Users(2L, "name_2", DateUtil.getDate("20140102"), DaoTestHelper.getDate("20150401123456")),
                new Users(3L, "name_3", DateUtil.getDate("20140103"), DaoTestHelper.getDate("20150401123456"))
        );

        ResultSetIterator rs = connection.prepareStatement("SELECT * FROM DAO_USERS ORDER BY USER_ID")
                .executeQuery();
        return new DeferredEntityList<SqlRow>(SqlRow.class, new SqlResourceHolder(rs));
    }
}

