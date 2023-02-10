package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.GenerationType;
import javax.persistence.OptimisticLockException;

import org.hamcrest.CoreMatchers;

import nablarch.common.dao.DaoTestHelper.Address;
import nablarch.common.dao.DaoTestHelper.AutoGenUsers;
import nablarch.common.dao.DaoTestHelper.IdentityGenUsers;
import nablarch.common.dao.DaoTestHelper.Users;
import nablarch.common.dao.DaoTestHelper.Users2;
import nablarch.common.dao.DaoTestHelper.Users3;
import nablarch.common.idgenerator.IdGenerator;
import nablarch.core.db.DbAccessException;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.ResultSetIterator;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.DateUtil;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import mockit.Expectations;
import mockit.Mocked;

/**
 * {@link BasicDaoContext}のテストクラス
 */
@RunWith(DatabaseTestRunner.class)
public class BasicDaoContextTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /** テストで使用するデータベース接続 */
    private TransactionManagerConnection connection;

    /** テスト対象 */
    private BasicDaoContext sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect());

    @Mocked
    private IdGenerator mockSequenceIdGenerator;

    @Mocked
    private IdGenerator mockTableIdGenerator;

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(Users.class);
        VariousDbTestHelper.createTable(Users2.class);
        VariousDbTestHelper.createTable(Users3.class);
        VariousDbTestHelper.createTable(Address.class);
    }

    @Before
    public void setUp() throws Exception {
        final ConnectionFactory connectionFactory = repositoryResource.getComponent("connectionFactory");
        connection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.SEQUENCE, mockSequenceIdGenerator);
        sut.setIdGenerator(GenerationType.TABLE, mockTableIdGenerator);

        DbConnectionContext.setConnection(connection);

        VariousDbTestHelper.delete(Users.class);
        VariousDbTestHelper.delete(Users2.class);
        VariousDbTestHelper.delete(Address.class);
    }

    @After
    public void tearDown() throws Exception {
        DbConnectionContext.removeConnection();
    }

    @After
    public void closeConnection() throws Exception {
        if (connection != null) {
            connection.terminate();
        }
    }

    /**
     * 単一の主キーを持つテーブルを
     * {@link BasicDaoContext#findById(Class, Object...)}で検索するケース。
     */
    @Test
    public void findById_singleKey() throws Exception {

        VariousDbTestHelper.setUpTable(
                new Users(100L, "なまえ_100", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 99L),
                new Users(101L, "なまえ_101", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(102L, "なまえ_102", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"), 99L));

        Users user = sut.findById(Users.class, 101);
        assertThat(user.getId(), is(101L));
        assertThat(user.getName(), is("なまえ_101"));
        assertThat(user.getBirthday(), is(DateUtil.getDate("20120102")));
        assertThat(user.getInsertDate(), is(DaoTestHelper.getDate("20150402123456")));
    }

    /**
     * 単一の主キーを持つテーブルを
     * {@link BasicDaoContext#findByIdOrNull(Class, Object...)}で検索するケース。
     */
    @Test
    public void findByIdOrNull_singleKey() {

        VariousDbTestHelper.setUpTable(
                new Users(100L, "なまえ_100", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 99L),
                new Users(101L, "なまえ_101", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(102L, "なまえ_102", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"), 99L));

        Users user = sut.findByIdOrNull(Users.class, 101);
        assertThat(user.getId(), is(101L));
        assertThat(user.getName(), is("なまえ_101"));
        assertThat(user.getBirthday(), is(DateUtil.getDate("20120102")));
        assertThat(user.getInsertDate(), is(DaoTestHelper.getDate("20150402123456")));
    }

    /**
     * 単一の主キーを持つテーブルを
     * {@link BasicDaoContext#findById(Class, Object...)}で検索するケース。
     */
    @Test
    public void findById_multipleKey() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Address(100L, "1", 1L, "1001001", "東京都新宿区・・・")
        );
        Address address = sut.findById(Address.class, 100, "1");
        assertThat(address.getId(), is(100L));
        assertThat(address.getCode(), is("1"));
        assertThat(address.getUserId(), is(1L));
        assertThat(address.getPostNo(), is("1001001"));
        assertThat(address.getAddress(), is("東京都新宿区・・・"));
    }

    /**
     * 単一の主キーを持つテーブルを
     * {@link BasicDaoContext#findByIdOrNull(Class, Object...)}で検索するケース。
     */
    @Test
    public void findByIdOrNull_multipleKey() {
        VariousDbTestHelper.setUpTable(
                new Address(100L, "1", 1L, "1001001", "東京都新宿区・・・")
        );
        Address address = sut.findByIdOrNull(Address.class, 100, "1");
        assertThat(address.getId(), is(100L));
        assertThat(address.getCode(), is("1"));
        assertThat(address.getUserId(), is(1L));
        assertThat(address.getPostNo(), is("1001001"));
        assertThat(address.getAddress(), is("東京都新宿区・・・"));
    }

    /**
     * {@link BasicDaoContext#findById(Class, Object...)}でデータが存在しない場合のケース。
     * <p/>
     * {@link NoDataException}が送出されること
     */
    @Test(expected = NoDataException.class)
    public void findByIdDataNotFound() throws Exception {
        sut.findById(Users.class, 100);
    }

    /**
     * {@link BasicDaoContext#findByIdOrNull(Class, Object...)}でデータが存在しない場合のケース。
     * <p/>
     * nullがかえされること。
     */
    @Test
    public void findByIdOrNullDataNotFound() {
        Users user = sut.findByIdOrNull(Users.class, 100);
        assertThat(user, nullValue());
    }

    /**
     * {@link BasicDaoContext#findById(Class, Object...)}で主キーの数と指定した条件数が一致しない場合のケース
     * <p/>
     * {@link IllegalArgumentException}が送出されること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void findByIdMismatchIdColumnCount() {
        sut.findById(Users.class);
    }

    /**
     * {@link BasicDaoContext#findByIdOrNull(Class, Object...)}で主キーの数と指定した条件数が一致しない場合のケース
     * <p/>
     * {@link IllegalArgumentException}が送出されること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void findByIdOrNullMismatchIdColumnCount() {
        sut.findByIdOrNull(Users.class);
    }

    /**
     * {@link BasicDaoContext#findAll(Class)}でデータが存在する場合のケース
     */
    @Test
    public void findAll() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(10L, "name_10", DateUtil.getDate("20140101"), DaoTestHelper.getDate("20150401123456")),
                new Users(11L, "name_11", DateUtil.getDate("20140102"), DaoTestHelper.getDate("20150402123456")),
                new Users(12L, "name_11", DateUtil.getDate("20140103"), DaoTestHelper.getDate("20150403123456"))
        );

        EntityList<Users> users = sut.findAll(Users.class);

        assertThat("遅延ロードではないので、結果オブジェクトはDeferredEntityListではないこと",
                users, is(not(instanceOf(DeferredEntityList.class))));

        assertThat("結果は3レコード取得できる", users.size(), is(3));

        // アサート用にソートする。
        ArrayList<Users> sortedUsers = new ArrayList<Users>(users);
        Collections.sort(sortedUsers, new Comparator<Users>() {
            @Override
            public int compare(Users o1, Users o2) {
                return o1.getId()
                        .compareTo(o2.getId());
            }
        });
        assertThat(sortedUsers.get(0)
                .getId(), is(10L));
        assertThat(sortedUsers.get(1)
                .getId(), is(11L));
        assertThat(sortedUsers.get(2)
                .getId(), is(12L));
    }

    /**
     * {@link BasicDaoContext#findAll(Class)}でデータが存在しない場合のケース
     * <p/>
     * サイズ0の結果がかえされること。
     */
    @Test
    public void findAllDataNotFound() throws Exception {
        EntityList<Users> users = sut.findAll(Users.class);
        assertThat(users.isEmpty(), is(true));
    }

    /**
     * {@link BasicDaoContext#findAll(Class)}で遅延ロード設定を有効にした場合のケース
     */
    @Test
    public void findAllDefer() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20140101"), DaoTestHelper.getDate("20150401123456")),
                new Users(2L, "name_2", DateUtil.getDate("20140102"), DaoTestHelper.getDate("20150402123456")),
                new Users(3L, "name_3", DateUtil.getDate("20140103"), DaoTestHelper.getDate("20150403123456"))
        );

        sut.defer();            // 遅延ロードを有効化
        DeferredEntityList<Users> users = (DeferredEntityList<Users>) sut.findAll(
                Users.class);

        assertThat("遅延ロードを表すDeferredEntityListが返却されること",
                users, is(instanceOf(DeferredEntityList.class)));

        for (Users user : users) {
            assertThat("対象データのいずれかであること", user.getId(), CoreMatchers.anyOf(is(1L), is(2L), is(3L)));
        }
        users.close();
    }

    /**
     * {@link BasicDaoContext#findAllBySqlFile(Class, String, Object)}、{@link BasicDaoContext#findAllBySqlFile(Class, String)}でページング設定なしのケース
     */
    @Test
    public void findAllBySqlFile_NotPaginate() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "なまえ_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456")),
                new Users(2L, "なまえ_2", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456")),
                new Users(3L, "なまえ_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"))
        );
        Object配列を条件に:
        {
            EntityList<Users> users = sut.findAllBySqlFile(Users.class, "FIND_USERS",
                    new Object[] {"なまえ_2", 2});
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(1));
            assertThat(users.get(0)
                    .getId(), is(2L));
        }

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setName("なまえ_2");
            cond.setId(2L);
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_WHERE_ENTITY", cond);
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(1));
            assertThat(users.get(0)
                    .getId(), is(2L));
        }

        Mapを条件に:
        {
            Map<String, Object> cond = new HashMap<String, Object>();
            cond.put("name", "なまえ_2");
            cond.put("id", 2);
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_WHERE_ENTITY", cond);
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(1));
            assertThat(users.get(0)
                    .getId(), is(2L));
        }

        条件なし:
        {
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_ALL_NOT_COND");
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(3));
            assertThat(users.get(0)
                    .getId(), is(1L));
            assertThat(users.get(1)
                    .getId(), is(2L));
            assertThat(users.get(2)
                    .getId(), is(3L));
        }

        Entityを条件に結果をSqlRowで取得:
        {
            Users cond = new Users();
            cond.setName("なまえ_2");
            cond.setId(2L);
            EntityList<SqlRow> users = sut.findAllBySqlFile(
                    SqlRow.class, "nablarch.common.dao.Result_SqlRow#FIND_USERS_WHERE_ENTITY", cond);

            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(1));
            assertThat(users.get(0)
                    .getLong("USER_ID"), is(2L));
        }
    }


    /**
     * {@link BasicDaoContext#findAllBySqlFile(Class, String, Object)}、{@link BasicDaoContext#findAllBySqlFile(Class, String)}でページング設定なしかつ、サロゲートペア有りのケース。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void findAllBySqlFile_NotPaginate_surrogatePair() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "なまえ_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456")),
                new Users(2L, "なまえ_2\uD840\uDC0B", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456")),
                new Users(3L, "なまえ_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"))
        );
        Object配列を条件に:
        {
            EntityList<Users> users = sut.findAllBySqlFile(Users.class, "FIND_USERS",
                    new Object[] {"なまえ_2\uD840\uDC0B", 2});
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(1));
            assertThat(users.get(0)
                    .getId(), is(2L));
        }

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setName("なまえ_2\uD840\uDC0B");
            cond.setId(2L);
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_WHERE_ENTITY", cond);
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(1));
            assertThat(users.get(0)
                    .getId(), is(2L));
        }

        Mapを条件に:
        {
            Map<String, Object> cond = new HashMap<String, Object>();
            cond.put("name", "なまえ_2\uD840\uDC0B");
            cond.put("id", 2);
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_WHERE_ENTITY", cond);
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(1));
            assertThat(users.get(0)
                    .getId(), is(2L));
        }

        条件なし:
        {
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_ALL_NOT_COND");
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(3));
            assertThat(users.get(0)
                    .getId(), is(1L));
            assertThat(users.get(1)
                    .getId(), is(2L));
            assertThat(users.get(2)
                    .getId(), is(3L));
        }

        Entityを条件に結果をSqlRowで取得:
        {
            Users cond = new Users();
            cond.setName("なまえ_2\uD840\uDC0B");
            cond.setId(2L);
            EntityList<SqlRow> users = sut.findAllBySqlFile(
                    SqlRow.class, "nablarch.common.dao.Result_SqlRow#FIND_USERS_WHERE_ENTITY", cond);

            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(1));
            assertThat(users.get(0)
                    .getLong("USER_ID"), is(2L));
        }
    }

    /**
     * {@link BasicDaoContext#findAllBySqlFile(Class, String, Object)}、{@link BasicDaoContext#findAllBySqlFile(Class, String)}でページングなしかつ遅延ロードのケース。
     */
    @Test
    public void findAllBySqlFile_NotPaginate_Defer() throws Exception {

        VariousDbTestHelper.setUpTable(
                new Users(1L, "なまえ_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456")),
                new Users(2L, "なまえ_2", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456")),
                new Users(3L, "なまえ_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"))
        );

        sut.defer();

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setName("なまえ_2");
            cond.setId(2L);
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_WHERE_ENTITY", cond);
            assertThat("遅延ロードなので、DeferredEntityListであること",
                    users, is(instanceOf(DeferredEntityList.class)));

            Iterator<Users> iterator = users.iterator();
            assertThat("データが取得出来ていること", iterator.hasNext(), is(true));
            assertThat(iterator.next()
                    .getId(), is(2L));
        }

        条件なし:
        {
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_ALL_NOT_COND");
            assertThat("遅延ロードなので、DeferredEntityListであること",
                    users, is(instanceOf(DeferredEntityList.class)));

            Iterator<Users> iterator = users.iterator();
            assertThat(iterator.next()
                    .getId(), is(1L));
            assertThat(iterator.next()
                    .getId(), is(2L));
            assertThat(iterator.next()
                    .getId(), is(3L));
            assertThat("全て読み終わったので次のレコードは存在しない", iterator.hasNext(), is(false));
        }

        Entityを条件にSqlRowを取得:
        {
            Users cond = new Users();
            cond.setName("なまえ_2");
            cond.setId(2L);
            EntityList<SqlRow> users = sut.findAllBySqlFile(
                    SqlRow.class, "nablarch.common.dao.Result_SqlRow#FIND_USERS_WHERE_ENTITY", cond);
            assertThat("遅延ロードなので、DeferredEntityListであること",
                    users, is(instanceOf(DeferredEntityList.class)));

            Iterator<SqlRow> iterator = users.iterator();
            assertThat("データが取得出来ていること", iterator.hasNext(), is(true));
            assertThat(iterator.next()
                    .getLong("USER_ID"), is(2L));
        }
    }


    /**
     * {@link BasicDaoContext#findAllBySqlFile(Class, String, Object)}、{@link BasicDaoContext#findAllBySqlFile(Class, String)}でページング設定ありのケース
     */
    @Test
    public void findAllBySqlFile_Paginate() throws Exception {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 30; i++) {
            long index = i + 1;
            VariousDbTestHelper.insert(
                    new Users(index, "なまえ_" + index, DateUtil.getDate(String.valueOf(20120100 + index)),
                            DaoTestHelper.getDate("20150401123456"))
            );
        }

        sut.page(2);
        sut.per(2);
        Object配列を条件に:
        {
            EntityList<Users> users = sut.findAllBySqlFile(Users.class, "FIND_USERS_ALL",
                    new Object[] {"なまえ_%"});

            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));
            assertThat("データが取得できること", users.size(), is(2));
            assertThat(users.get(0)
                    .getId(), is(3L));
            assertThat(users.get(1)
                    .getId(), is(4L));

            assertThat("ページング情報が取得できること", users.getPagination()
                    .getResultCount(), is(30));
            assertThat("ページング情報が取得できること", users.getPagination()
                    .getPageNumber(), is(2));
            assertThat("ページング情報が取得できること", users.getPagination()
                    .getStartPosition(), is(3));
        }

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setName("なまえ_");
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_ALL_WHERE_ENTITY", cond);

            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));
            assertThat("データが取得できること", users.size(), is(2));
            assertThat(users.get(0)
                    .getId(), is(3L));
            assertThat(users.get(1)
                    .getId(), is(4L));
        }

        Mapを条件に:
        {
            Map<String, Object> cond = new HashMap<String, Object>();
            cond.put("name", "なまえ_");
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_ALL_WHERE_ENTITY", cond);
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(2));
            assertThat(users.get(0)
                    .getId(), is(3L));
            assertThat(users.get(1)
                    .getId(), is(4L));
        }


        条件なし:
        {
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_ALL_NOT_COND");
            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));

            assertThat("データが取得できること", users.size(), is(2));
            assertThat(users.get(0)
                    .getId(), is(3L));
            assertThat(users.get(1)
                    .getId(), is(4L));
        }

        Entityを条件にSqlRowを取得するケース:
        {
            Users cond = new Users();
            cond.setName("なまえ_");
            EntityList<SqlRow> users = sut.findAllBySqlFile(
                    SqlRow.class, "nablarch.common.dao.Result_SqlRow#FIND_USERS_ALL_WHERE_ENTITY", cond);

            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));
            assertThat("データが取得できること", users.size(), is(2));
            assertThat(users.get(0)
                    .getLong("userId"), is(3L));
            assertThat(users.get(1)
                    .getLong("userId"), is(4L));
        }

        Entityを条件に_1ページの件数に満たないデータを取得するケース:
        {
            sut.page(8);
            sut.per(4);

            Users cond = new Users();
            cond.setName("なまえ_");
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_ALL_WHERE_ENTITY", cond);

            assertThat("遅延ロードではないので、DeferredEntityListではないこと",
                    users, is(not(instanceOf(DeferredEntityList.class))));
            assertThat("データが取得できること", users.size(), is(2));
            assertThat(users.get(0)
                    .getId(), is(29L));
            assertThat(users.get(1)
                    .getId(), is(30L));
        }
    }

    /**
     * {@link BasicDaoContext#findAllBySqlFile(Class, String, Object)}、{@link BasicDaoContext#findAllBySqlFile(Class, String)}でページング設定ありかつ遅延ロードのケース
     * <p/>
     * ページングありかつ遅延ロードはサポートしないので、{@link IllegalArgumentException}が送出されること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void findAllBySqlFile_Paginate_Defer() throws Exception {
        for (int i = 0; i < 30; i++) {
            long index = i + 1;
            VariousDbTestHelper.setUpTable(
                    new Users(index, "なまえ_" + index, DateUtil.getDate(String.valueOf(20120100 + index)),
                            DaoTestHelper.getDate("20150401123456"))
            );
        }

        sut.page(2);
        sut.per(2);
        sut.defer();

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setName("なまえ_");
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_ALL_WHERE_ENTITY", cond);

            try {
                assertThat("遅延ロードなので、DeferredEntityListであること",
                        users, is(instanceOf(DeferredEntityList.class)));
                fail("とおらない。");
            } catch (Exception e) {
                assertThat(e, is(instanceOf(IllegalArgumentException.class)));
            }
        }

        条件なし:
        {
            EntityList<Users> users = sut.findAllBySqlFile(Users.class,
                    "FIND_USERS_ALL_NOT_COND");

            try {
                assertThat("遅延ロードなので、DeferredEntityListであること",
                        users, is(instanceOf(DeferredEntityList.class)));
                fail("とおらない。");
            } catch (Exception e) {
                assertThat(e, is(instanceOf(IllegalArgumentException.class)));
            }
        }
    }

    /**
     * {@link BasicDaoContext#findBySqlFile(Class, String, Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void findBySqlFile() throws Exception {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 10; i++) {
            long index = i + 1;
            VariousDbTestHelper.insert(
                    new Users(index, "なまえ_" + index, DateUtil.getDate(String.valueOf(20140100 + index)),
                            DaoTestHelper.getDate("20150401123456"))
            );
        }

        Object配列を条件に:
        {
            Users user = sut.findBySqlFile(Users.class, "FIND_BY_ID_WHERE_ARRAY",
                    new Object[] {7L});
            assertThat(user.getId(), is(7L));
            assertThat(user.getName(), is("なまえ_7"));
        }

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setId(5L);
            Users user = sut.findBySqlFile(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);

            assertThat(user.getId(), is(5L));
            assertThat(user.getName(), is("なまえ_5"));
        }

        Mapを条件に:
        {
            HashMap<String, Object> cond = new HashMap<String, Object>();
            cond.put("id", 3);
            Users user = sut.findBySqlFile(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);

            assertThat(user.getId(), is(3L));
            assertThat(user.getName(), is("なまえ_3"));
        }

        Entityを条件にSqlRowを取得:
        {
            Users cond = new Users();
            cond.setId(5L);
            SqlRow user = sut.findBySqlFile(SqlRow.class, "nablarch.common.dao.Result_SqlRow#FIND_BY_ID_WHERE_ENTITY",
                    cond);

            assertThat(user.getLong("userId"), is(5L));
            assertThat(user.getString("name"), is("なまえ_5"));
        }
    }

    /**
     * {@link BasicDaoContext#findBySqlFileOrNull(Class, String, Object)}のテスト。
     */
    @Test
    public void findBySqlFileOrNull() {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 10; i++) {
            long index = i + 1;
            VariousDbTestHelper.insert(
                    new Users(index, "なまえ_" + index, DateUtil.getDate(String.valueOf(20140100 + index)),
                            DaoTestHelper.getDate("20150401123456"))
            );
        }

        Object配列を条件に:
        {
            Users user = sut.findBySqlFileOrNull(Users.class, "FIND_BY_ID_WHERE_ARRAY",
                    new Object[] { 7L });
            assertThat(user.getId(), is(7L));
            assertThat(user.getName(), is("なまえ_7"));
        }

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setId(5L);
            Users user = sut.findBySqlFileOrNull(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);

            assertThat(user.getId(), is(5L));
            assertThat(user.getName(), is("なまえ_5"));
        }

        Mapを条件に:
        {
            HashMap<String, Object> cond = new HashMap<String, Object>();
            cond.put("id", 3);
            Users user = sut.findBySqlFileOrNull(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);

            assertThat(user.getId(), is(3L));
            assertThat(user.getName(), is("なまえ_3"));
        }

        Entityを条件にSqlRowを取得:
        {
            Users cond = new Users();
            cond.setId(5L);
            SqlRow user = sut.findBySqlFileOrNull(SqlRow.class, "nablarch.common.dao.Result_SqlRow#FIND_BY_ID_WHERE_ENTITY",
                    cond);

            assertThat(user.getLong("userId"), is(5L));
            assertThat(user.getString("name"), is("なまえ_5"));
        }
    }

    /**
     * {@link BasicDaoContext#findBySqlFile(Class, String, Object)}のサロゲートペア有のテスト。
     *
     * @throws Exception
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void findBySqlFile_surrogatePair() throws Exception {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 10; i++) {
            long index = i + 1;
            VariousDbTestHelper.insert(
                    new Users(index, "なまえ\uD840\uDC0B_" + index, DateUtil.getDate(String.valueOf(20140100 + index)),
                            DaoTestHelper.getDate("20150401123456"))
            );
        }

        Object配列を条件に:
        {
            Users user = sut.findBySqlFile(Users.class, "FIND_BY_ID_WHERE_ARRAY",
                    new Object[] {7L});
            assertThat(user.getId(), is(7L));
            assertThat(user.getName(), is("なまえ\uD840\uDC0B_7"));
        }

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setId(5L);
            Users user = sut.findBySqlFile(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);

            assertThat(user.getId(), is(5L));
            assertThat(user.getName(), is("なまえ\uD840\uDC0B_5"));
        }

        Mapを条件に:
        {
            HashMap<String, Object> cond = new HashMap<String, Object>();
            cond.put("id", 3);
            Users user = sut.findBySqlFile(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);

            assertThat(user.getId(), is(3L));
            assertThat(user.getName(), is("なまえ\uD840\uDC0B_3"));
        }

        Entityを条件にSqlRowを取得:
        {
            Users cond = new Users();
            cond.setId(5L);
            SqlRow user = sut.findBySqlFile(SqlRow.class, "nablarch.common.dao.Result_SqlRow#FIND_BY_ID_WHERE_ENTITY",
                    cond);

            assertThat(user.getLong("userId"), is(5L));
            assertThat(user.getString("name"), is("なまえ\uD840\uDC0B_5"));
        }
    }

    /**
     * {@link BasicDaoContext#findBySqlFileOrNull(Class, String, Object)}のサロゲートペア有のテスト。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void findBySqlFileOrNull_surrogatePair() {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 10; i++) {
            long index = i + 1;
            VariousDbTestHelper.insert(
                    new Users(index, "なまえ\uD840\uDC0B_" + index, DateUtil.getDate(String.valueOf(20140100 + index)),
                            DaoTestHelper.getDate("20150401123456"))
            );
        }

        Object配列を条件に:
        {
            Users user = sut.findBySqlFileOrNull(Users.class, "FIND_BY_ID_WHERE_ARRAY",
                    new Object[] { 7L });
            assertThat(user.getId(), is(7L));
            assertThat(user.getName(), is("なまえ\uD840\uDC0B_7"));
        }

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setId(5L);
            Users user = sut.findBySqlFileOrNull(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);

            assertThat(user.getId(), is(5L));
            assertThat(user.getName(), is("なまえ\uD840\uDC0B_5"));
        }

        Mapを条件に:
        {
            HashMap<String, Object> cond = new HashMap<String, Object>();
            cond.put("id", 3);
            Users user = sut.findBySqlFileOrNull(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);

            assertThat(user.getId(), is(3L));
            assertThat(user.getName(), is("なまえ\uD840\uDC0B_3"));
        }

        Entityを条件にSqlRowを取得:
        {
            Users cond = new Users();
            cond.setId(5L);
            SqlRow user = sut.findBySqlFileOrNull(SqlRow.class, "nablarch.common.dao.Result_SqlRow#FIND_BY_ID_WHERE_ENTITY",
                    cond);

            assertThat(user.getLong("userId"), is(5L));
            assertThat(user.getString("name"), is("なまえ\uD840\uDC0B_5"));
        }
    }

    /**
     * {@link BasicDaoContext#findBySqlFile(Class, String, Object)}でデータが存在しない場合のケース。
     * <p/>
     * {@link NoDataException}が送出されること。
     *
     * @throws Exception
     */
    @Test(expected = NoDataException.class)
    public void findBySqlFile_DataNotFound() throws Exception {
        for (int i = 0; i < 5; i++) {
            long index = i + 1;
            VariousDbTestHelper.setUpTable(
                    new Users(index, "name_" + index, DateUtil.getDate(String.valueOf(20140100 + index)),
                            DaoTestHelper.getDate("20150401123456"))
            );
        }

        Users cond = new Users();
        cond.setId(6L);
        sut.findBySqlFile(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);
    }

    /**
     * {@link BasicDaoContext#findBySqlFileOrNull(Class, String, Object)}でデータが存在しない場合のケース。
     * <p/>
     * nullがかえされること。
     */
    @Test
    public void findBySqlFileOrNull_DataNotFound() {
        for (int i = 0; i < 5; i++) {
            long index = i + 1;
            VariousDbTestHelper.setUpTable(
                    new Users(index, "name_" + index, DateUtil.getDate(String.valueOf(20140100 + index)),
                            DaoTestHelper.getDate("20150401123456"))
            );
        }

        Users cond = new Users();
        cond.setId(6L);
        Users user = sut.findBySqlFileOrNull(Users.class, "FIND_BY_ID_WHERE_ENTITY", cond);

        assertThat(user, nullValue());
    }

    /**
     * {@link BasicDaoContext#countBySqlFile(Class, String, Object)}のテスト。
     */
    @Test
    public void countBySqlFile() throws Exception {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 20; i++) {
            long index = i + 1;
            VariousDbTestHelper.insert(
                    new Users(index, "name_" + index, DateUtil.getDate(String.valueOf(20141200 + index)),
                            DaoTestHelper.getDate("20150401123456"))
            );
        }

        Object配列を条件に:
        {
            long count = sut.countBySqlFile(Users.class, "FIND_USERS_ALL", new Object[] {"name\\_1_"});

            assertThat(count, is(10L));
        }

        Entityを条件に:
        {
            Users cond = new Users();
            cond.setName("name_3");

            long count = sut.countBySqlFile(Users.class, "FIND_USERS_ALL_WHERE_ENTITY", cond);
            assertThat(count, is(1L));
        }

        Mapを条件に:
        {

            HashMap<String, Object> cond = new HashMap<String, Object>();
            cond.put("name", "name_1");

            long count = sut.countBySqlFile(Users.class, "FIND_USERS_ALL_WHERE_ENTITY", cond);
            assertThat(count, is(11L));
        }
    }

    /**
     * 件数取得のSQLが結果を返さない場合のテスト。
     *
     * ※通常はありえない
     */
    @Test(expected = IllegalStateException.class)
    public void countBySqlFile_DataNotFound() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return true;
            }
        });

        new Expectations(connection) {{
            ParameterizedSqlPStatement statement = connection.prepareParameterizedCountSqlStatementBySqlId(
                    anyString, any);
            ResultSetIterator keys = statement.executeQueryByMap((Map<String, Object>) any);
            keys.next();
            result = false;
        }};
        sut.setDbConnection(connection);

        HashMap<String, Object> cond = new HashMap<String, Object>();
        cond.put("name", "name_1");

        sut.countBySqlFile(Users.class, "FIND_USERS_ALL_WHERE_ENTITY", cond);
    }

    /**
     * {@link BasicDaoContext#update(Object)}のテスト。
     */
    @Test
    public void update() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 99L),
                new Users(2L, "name_2", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(3L, "name_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"), 99L));

        Users user = new Users();
        user.setId(2L);
        user.setName("名前を更新");
        user.setBirthday(DateUtil.getDate("20000101"));
        user.setInsertDate(DaoTestHelper.getDate("20151231235959"));
        user.setVersion(99L);

        int updatedCount = sut.update(user);
        assertThat("更新件数は、1", updatedCount, is(1));

        Users actual = sut.findById(Users.class, 2L);
        assertThat(actual.getId(), is(2L));
        assertThat(actual.getName(), is("名前を更新"));
        assertThat(actual.getBirthday(), is(DateUtil.getDate("20000101")));
        assertThat(actual.getInsertDate(), is(DaoTestHelper.getDate("20151231235959")));
        assertThat(actual.getVersion(), is(100L));
    }

    /**
     * {@link BasicDaoContext#update(Object)}のサロゲートペア有のテスト。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void update_surrogatePair() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 99L),
                new Users(2L, "name_2", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(3L, "name_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"), 99L));

        Users user = new Users();
        user.setId(2L);
        user.setName("名前を更新\uD840\uDC0B");
        user.setBirthday(DateUtil.getDate("20000101"));
        user.setInsertDate(DaoTestHelper.getDate("20151231235959"));
        user.setVersion(99L);

        int updatedCount = sut.update(user);
        assertThat("更新件数は、1", updatedCount, is(1));

        Users actual = sut.findById(Users.class, 2L);
        assertThat(actual.getId(), is(2L));
        assertThat(actual.getName(), is("名前を更新\uD840\uDC0B"));
        assertThat(actual.getBirthday(), is(DateUtil.getDate("20000101")));
        assertThat(actual.getInsertDate(), is(DaoTestHelper.getDate("20151231235959")));
        assertThat(actual.getVersion(), is(100L));
    }

    /**
     * {@link BasicDaoContext#update(Object)}で更新対象データが存在しない場合のケース
     *
     * @throws Exception
     */
    @Test
    public void update_optimisticLockException() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 99L),
                new Users(2L, "name_2", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(3L, "name_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"), 99L));

        バージョン不一致:
        {
            Users user = new Users();
            user.setId(2L);
            user.setName("名前を更新");
            user.setBirthday(DateUtil.getDate("20000101"));
            user.setInsertDate(DaoTestHelper.getDate("20151231235959"));
            user.setVersion(98L);       // バージョン不一致
            try {
                sut.update(user);
                fail("バージョン不一致なのでここは通らない");
            } catch (Exception e) {
                assertThat(e, is(instanceOf(OptimisticLockException.class)));
            }
        }

        バージョンカラムがあるテーブルで更新対象データなし:
        {
            Users user = new Users();
            user.setId(4L); // 更新対象データは存在しない
            user.setName("名前を更新");
            user.setBirthday(DateUtil.getDate("20000101"));
            user.setInsertDate(DaoTestHelper.getDate("20151231235959"));
            user.setVersion(99L);
            try {
                sut.update(user);
                fail("更新対象データなしなのでここには来ない");
            } catch (Exception e) {
                assertThat(e, is(instanceOf(OptimisticLockException.class)));
            }
        }

        バージョンカラムが無いテーブルで更新対象データなし:
        {
            Address address = new Address(1L, "1", 1L, "3790001", "住所");
            int updatedCount = sut.update(address);

            // バージョンカラムが存在しないテーブルは更新対象なしでもエラーとならない
            assertThat("更新対象件数は0", updatedCount, is(0));
        }
    }

    /**
     * {@link BasicDaoContext#batchUpdate(java.util.List)}のテスト。
     */
    @Test
    public void batchUpdate() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 98L),
                new Users(2L, "name_2", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(3L, "name_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"), 100L));

        sut.batchUpdate(
                Arrays.asList(
                        new Users(3L, "名前を更新_3", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20151231235959"), 100L),
                        new Users(2L, "名前を更新_2", DateUtil.getDate("20000102"), DaoTestHelper.getDate("20161231235959"), 99L),
                        new Users(1L, "名前を更新_1", DateUtil.getDate("20000102"), DaoTestHelper.getDate("20161231235959"), 97L)        // バージョン番号不一致で更新されない
                ));

        final Users user1 = sut.findById(Users.class, 1L);
        assertThat("更新されていないこと", user1.getVersion(), is(98L));

        // -------------------------------------------------- ユーザ２：更新されていること
        Users user2 = sut.findById(Users.class, 2L);
        assertThat(user2.getId(), is(2L));
        assertThat(user2.getName(), is("名前を更新_2"));
        assertThat(user2.getBirthday(), is(DateUtil.getDate("20000102")));
        assertThat(user2.getInsertDate(), is(DaoTestHelper.getDate("20161231235959")));
        assertThat(user2.getVersion(), is(100L));

        // -------------------------------------------------- ユーザ２：更新されていること
        final Users user3 = sut.findById(Users.class, 3L);
        assertThat(user3.getId(), is(3L));
        assertThat(user3.getName(), is("名前を更新_3"));
        assertThat(user3.getBirthday(), is(DateUtil.getDate("20000101")));
        assertThat(user3.getInsertDate(), is(DaoTestHelper.getDate("20151231235959")));
        assertThat(user3.getVersion(), is(101L));
    }

    /**
     * {@link BasicDaoContext#batchUpdate(java.util.List)}のサロゲートペア有テスト。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void batchUpdate_surrogatePair() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 98L),
                new Users(2L, "name_2\uD840\uDC0B", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(3L, "name_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"), 100L));

        sut.batchUpdate(
                Arrays.asList(
                        new Users(3L, "名前を更新_3", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20151231235959"), 100L),
                        new Users(2L, "名前を更新_2\uD840\uDC0B", DateUtil.getDate("20000102"), DaoTestHelper.getDate("20161231235959"), 99L),
                        new Users(1L, "名前を更新_1", DateUtil.getDate("20000102"), DaoTestHelper.getDate("20161231235959"), 97L)        // バージョン番号不一致で更新されない
                ));

        final Users user1 = sut.findById(Users.class, 1L);
        assertThat("更新されていないこと", user1.getVersion(), is(98L));

        // -------------------------------------------------- ユーザ２：更新されていること
        Users user2 = sut.findById(Users.class, 2L);
        assertThat(user2.getId(), is(2L));
        assertThat(user2.getName(), is("名前を更新_2\uD840\uDC0B"));
        assertThat(user2.getBirthday(), is(DateUtil.getDate("20000102")));
        assertThat(user2.getInsertDate(), is(DaoTestHelper.getDate("20161231235959")));
        assertThat(user2.getVersion(), is(100L));

        // -------------------------------------------------- ユーザ２：更新されていること
        final Users user3 = sut.findById(Users.class, 3L);
        assertThat(user3.getId(), is(3L));
        assertThat(user3.getName(), is("名前を更新_3"));
        assertThat(user3.getBirthday(), is(DateUtil.getDate("20000101")));
        assertThat(user3.getInsertDate(), is(DaoTestHelper.getDate("20151231235959")));
        assertThat(user3.getVersion(), is(101L));
    }

    /**
     * 一括更新で空のリストを指定した場合、処理は正常に終了すること。（処理はなにも実行されない。)
     */
    @Test
    public void batchUpdate_emptyList() throws Exception {
        final Users user1 = new Users(
                1L, "name1", DaoTestHelper.getDate("20150817"), DaoTestHelper.getDate("20150818"));

        VariousDbTestHelper.setUpTable(user1);

        sut.setDbConnection(connection);
        sut.batchUpdate(new ArrayList<Users>());

        final List<Users> users = VariousDbTestHelper.findAll(Users.class);
        assertThat("値が変更されていないこと", users, contains(user1));
    }

    /**
     * {@link BasicDaoContext#insert(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void insert() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.SEQUENCE, mockSequenceIdGenerator);
        sut.setIdGenerator(GenerationType.TABLE, mockTableIdGenerator);

        new Expectations() {{
            mockSequenceIdGenerator.generateId("USER_ID_SEQ");
            returns("1", "1");
            mockTableIdGenerator.generateId("ADDRESS_ID_SEQ");
            returns("1", "1");
        }};

        バージョンカラムが存在しているテーブル:
        {
            Users user = new Users();
            user.setName("なまえ");
            user.setBirthday(DateUtil.getDate("19900101"));
            user.setInsertDate(DaoTestHelper.getDate("20000101010101"));
            sut.insert(user);

            Users actual = sut.findById(Users.class, 1L);
            assertThat(actual.getId(), is(1L));
            assertThat(actual.getName(), is("なまえ"));
            assertThat(actual.getBirthday(), is(DateUtil.getDate("19900101")));
            assertThat(actual.getInsertDate(), is(DaoTestHelper.getDate("20000101010101")));
            assertThat(actual.getVersion(), is(0L));
        }

        バージョンカラムが存在していないテーブル:
        {
            Address address = new Address();
            address.setCode("2");
            address.setUserId(10L);
            address.setPostNo("1231234");
            address.setAddress("住所");

            sut.insert(address);

            Address actual = sut.findById(Address.class, 1L, "2");
            assertThat(actual.getId(), is(1L));
            assertThat(actual.getCode(), is("2"));
            assertThat(actual.getUserId(), is(10L));
            assertThat(actual.getPostNo(), is("1231234"));
            assertThat(actual.getAddress(), is("住所"));
        }

        バージョンカラムがNumber互換型ではないテーブル:
        {
            Users2 user = new Users2();
            user.setId(100L);
            user.setVersion("99");
            user.setName("name");
            user.setBirthday(DateUtil.getDate("19001212"));
            user.setInsertDate(DaoTestHelper.getDate("20001231010101"));
            sut.insert(user);

            Users2 actual = sut.findById(Users2.class, 100L);
            assertThat(actual.getId(), is(100L));
            assertThat(actual.getName(), is("name"));
            assertThat(actual.getBirthday(), is(DateUtil.getDate("19001212")));
            assertThat(actual.getInsertDate(), is(DaoTestHelper.getDate("20001231010101")));
            assertThat(actual.getVersion(), is("99"));
        }

        IDおよびバージョンカラムがLongではないテーブル_insertの前処理で変換が必要:
        {
            Users3 user = new Users3();
            sut.insert(user);

            Users3 actual = sut.findById(Users3.class, 1);
            assertThat(actual.getId(), is(1));
            assertThat(actual.getVersion(), is(0));
        }
    }

    /**
     * {@link BasicDaoContext#insert(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void insert_surrogatePair() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.SEQUENCE, mockSequenceIdGenerator);
        sut.setIdGenerator(GenerationType.TABLE, mockTableIdGenerator);

        new Expectations() {{
            mockSequenceIdGenerator.generateId("USER_ID_SEQ");
            returns("1", "1");
        }};

        バージョンカラムが存在しているテーブル:
        {
            Users user = new Users();
            user.setName("なまえ\uD840\uDC0B");
            user.setBirthday(DateUtil.getDate("19900101"));
            user.setInsertDate(DaoTestHelper.getDate("20000101010101"));
            sut.insert(user);

            Users actual = sut.findById(Users.class, 1L);
            assertThat(actual.getId(), is(1L));
            assertThat(actual.getName(), is("なまえ\uD840\uDC0B"));
            assertThat(actual.getBirthday(), is(DateUtil.getDate("19900101")));
            assertThat(actual.getInsertDate(), is(DaoTestHelper.getDate("20000101010101")));
            assertThat(actual.getVersion(), is(0L));
        }
    }

    /**
     * シーケンスで採番してINSERTできること。
     *
     * @throws Exception
     */
    @Test
    public void insertFromSequenceGenerator() throws Exception {
        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.SEQUENCE, mockSequenceIdGenerator);

        Users users = new Users(null, "name", DateUtil.getDate("20140101"), DaoTestHelper.getDate("20150401123456"));

        new Expectations() {{
            mockSequenceIdGenerator.generateId("USER_ID_SEQ");
            returns("999", "9999");
        }};

        sut.insert(users);
        assertThat("採番された値が設定されていること", users.getId(), is(999L));

        Users user1 = sut.findById(Users.class, users.getId());
        assertThat(user1.getId(), is(users.getId()));
        assertThat(user1.getName(), is(users.getName()));
        assertThat(user1.getBirthday(), is(users.getBirthday()));
        assertThat(user1.getInsertDate(), is(users.getInsertDate()));

        users.setName("name2");
        sut.insert(users);
        assertThat("採番された値が設定されていること", users.getId(), is(9999L));

        Users user2 = sut.findById(Users.class, users.getId());
        assertThat(user2.getId(), is(users.getId()));
        assertThat(user2.getName(), is(users.getName()));
        assertThat(user2.getBirthday(), is(users.getBirthday()));
        assertThat(user2.getInsertDate(), is(users.getInsertDate()));
    }

    /**
     * 採番方法でsequenceを指定した場合で、ダイアレクトでサポートされていない場合。
     * <p/>
     * ダイアレクトでサポートされていないので例外が発生する。
     */
    @Test(expected = IllegalEntityException.class)
    public void insertFromSequenceGenerator_Unsupported() throws Exception {
        Users user = new Users(null, "name", DateUtil.getDate("20150115"), DaoTestHelper.getDate("20150401123456"));
        sut.insert(user);
    }

    /**
     * テーブルで採番してINSERTできること
     */
    @Test
    public void insertFromTableGenerator() throws Exception {
        new Expectations() {{
            mockTableIdGenerator.generateId("ADDRESS_ID_SEQ");
            returns("12345", "54321");
        }};
        Address address = new Address(null, "1", 1L, "1231234", "住所");
        sut.insert(address);

        assertThat("採番された値が設定されていること", address.getId(), is(12345L));

        Address address1 = sut.findById(Address.class, address.getId(), "1");
        assertThat(address1.getId(), is(address.getId()));
        assertThat(address1.getCode(), is("1"));
        assertThat(address1.getUserId(), is(1L));
        assertThat(address1.getPostNo(), is("1231234"));
        assertThat(address1.getAddress(), is("住所"));

        sut.insert(address);
        assertThat("insertの度に採番されること", address.getId(), is(54321L));
    }

    /**
     * GeneratedValueでidentityの場合で、採番された値が取得できること。
     */
    @Test
    public void insertFromIdentityGenerator() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return true;
            }
        });

        new Expectations(connection) {{
            SqlPStatement statement = connection.prepareStatement(anyString,(String[]) withNotNull());
            ResultSet keys = statement.getGeneratedKeys();
            keys.next();
            result = true;
            result = false;
            keys.getString(1);
            result = "12345";
        }};
        sut.setDbConnection(connection);
        IdentityGenUsers user = new IdentityGenUsers(
                null, "name", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));

        sut.insert(user);
        assertThat(user.getId(), is(12345L));
    }

    /**
     * GeneratedValueでidentityの場合で、ResultSet#close時の例外は無視されること(採番された値が取得できること。)
     */
    @Test
    public void insertFromIdentityGenerator_ResultSetCloseError() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return true;
            }
        });

        new Expectations(connection) {{
            SqlPStatement statement = connection.prepareStatement(anyString, (String[]) withNotNull());
            ResultSet keys = statement.getGeneratedKeys();
            keys.next();
            result = true;
            result = false;
            keys.getString(1);
            result = "54321";
            keys.close();
            result = new SQLException("error");
        }};
        sut.setDbConnection(connection);
        AutoGenUsers user = new AutoGenUsers(
                null, "name", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));

        sut.insert(user);
        assertThat(user.getId(), is(54321L));
    }

    /**
     * GeneratedValueでidentityの場合で、採番された値取得時にDB例外が発生した場合
     */
    @Test
    public void insertFromIdentity_getGeneratedKeyError() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return true;
            }
        });

        new Expectations(connection) {{
            SqlPStatement statement = connection.prepareStatement(anyString, (String[]) withNotNull());
            statement.getGeneratedKeys()
                    .next();
            result = new SQLException("error");
        }};
        sut.setDbConnection(connection);
        IdentityGenUsers user = new IdentityGenUsers(
                null, "name", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));

        RuntimeException exception = null;
        try {
            sut.insert(user);
        } catch (RuntimeException e) {
            exception = e;
        }
        assertThat(exception, is(instanceOf(DbAccessException.class)));
        assertThat(exception.getMessage(), containsString("failed to get auto generated key."));
    }


    /**
     * 採番方法でidentityを指定した場合で、ダイアレクトでサポートされていない場合。
     * <p/>
     * ダイアレクトでサポートsれていないので例外が発生する。
     */
    @Test(expected = IllegalEntityException.class)
    public void insertFromIdentity_unsupportedDialect() {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return false;
            }
        });
        sut.setDbConnection(connection);
        IdentityGenUsers user = new IdentityGenUsers(
                null, "name", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));

        sut.insert(user);
    }

    /**
     * 採番方法でautoを指定した場合でシーケンスをサポートするDialectの場合。
     * <p/>
     * シーケンスを使った採番処理が行われること。
     * なお、シーケンス名は「テーブル名 + '_' + カラム名」であること
     */
    @Test
    public void insertFromAuto_sequenceSupportDialect() throws Exception {
        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.SEQUENCE, mockSequenceIdGenerator);

        new Expectations() {{
            mockSequenceIdGenerator.generateId("DAO_USERS_USER_ID");
            returns("100", "200");
        }};

        AutoGenUsers user = new AutoGenUsers(null, "name", DateUtil.getDate("20150115"),
                DaoTestHelper.getDate("20150401123456"));
        sut.insert(user);

        AutoGenUsers actual = sut.findById(AutoGenUsers.class, user.getId());
        assertThat(actual.getName(), is(user.getName()));
        assertThat(actual.getBirthday(), is(user.getBirthday()));

        assertThat("採番された値が設定されていること", user.getId(), is(100L));

        sut.insert(user);
        assertThat("インサートあされるたびにIDが増加すること（増分値100指定)", user.getId(), is(200L));
    }

    /**
     * 採番方法でautoを指定した場合でシーケンスもIDENTITYもサポートしないダイアレクトの場合。
     * <p/>
     * テーブルを使った採番処理が行われること。 なお、テーブル採番の識別子は「テーブル名 + '_' + カラム名」であること
     */
    @Test
    public void insertFromAuto_tableSupportDialect() throws Exception {
        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return false;
            }

            @Override
            public boolean supportsSequence() {
                return false;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.TABLE, mockTableIdGenerator);

        new Expectations() {{
            mockTableIdGenerator.generateId("DAO_USERS_USER_ID");
            returns("101", null);
        }};

        AutoGenUsers users = new AutoGenUsers(null, "1", DateUtil.getDate("20000102"),
                DaoTestHelper.getDate("20150401123456"));
        sut.insert(users);

        assertThat("採番された値が設定されていること", users.getId(), is(101L));

        AutoGenUsers actual = sut.findById(AutoGenUsers.class, users.getId());
        assertThat(actual.getId(), is(users.getId()));
        assertThat(actual.getName(), is(users.getName()));
        assertThat(actual.getBirthday(), is(users.getBirthday()));
        assertThat(actual.getInsertDate(), is(users.getInsertDate()));
    }

    /**
     * {@link BasicDaoContext#batchInsert(java.util.List)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void batchInsert() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.SEQUENCE, mockSequenceIdGenerator);
        sut.setIdGenerator(GenerationType.TABLE, mockTableIdGenerator);

        new Expectations() {{
            mockSequenceIdGenerator.generateId("USER_ID_SEQ");
            returns("1", "3", "5");
            mockTableIdGenerator.generateId("ADDRESS_ID_SEQ");
            returns("1", "10");
        }};

        バージョンカラムが存在しているテーブル:
        {
            Users user1 = new Users();
            user1.setName("なまえ１");
            user1.setBirthday(DateUtil.getDate("19900101"));
            user1.setInsertDate(DaoTestHelper.getDate("20000101010101"));

            Users user2 = new Users();
            user2.setName("なまえ２");
            user2.setBirthday(DateUtil.getDate("19900102"));
            user2.setInsertDate(DaoTestHelper.getDate("20000101010102"));

            Users user3 = new Users();
            user3.setName("なまえ３");
            user3.setBirthday(DateUtil.getDate("19900103"));
            user3.setInsertDate(DaoTestHelper.getDate("20000101010103"));

            sut.batchInsert(Arrays.asList(user1, user2, user3));

            assertThat("3レコード登録されていること", sut.findAll(Users.class).size(), is(3));

            Users actual1 = sut.findById(Users.class, 1L);
            assertThat(actual1.getId(), is(1L));
            assertThat(actual1.getName(), is("なまえ１"));
            assertThat(actual1.getBirthday(), is(DateUtil.getDate("19900101")));
            assertThat(actual1.getInsertDate(), is(DaoTestHelper.getDate("20000101010101")));
            assertThat(actual1.getVersion(), is(0L));

            Users actual2 = sut.findById(Users.class, 3L);
            assertThat(actual2.getId(), is(3L));
            assertThat(actual2.getName(), is("なまえ２"));
            assertThat(actual2.getBirthday(), is(DateUtil.getDate("19900102")));
            assertThat(actual2.getInsertDate(), is(DaoTestHelper.getDate("20000101010102")));
            assertThat(actual2.getVersion(), is(0L));

            Users actual3 = sut.findById(Users.class, 5L);
            assertThat(actual3.getId(), is(5L));
            assertThat(actual3.getName(), is("なまえ３"));
            assertThat(actual3.getBirthday(), is(DateUtil.getDate("19900103")));
            assertThat(actual3.getInsertDate(), is(DaoTestHelper.getDate("20000101010103")));
            assertThat(actual3.getVersion(), is(0L));
        }

        バージョンカラムが存在していないテーブル:
        {
            Address address1 = new Address();
            address1.setCode("2");
            address1.setUserId(10L);
            address1.setPostNo("1231234");
            address1.setAddress("住所");
            Address address2 = new Address();
            address2.setCode("3");
            address2.setUserId(20L);
            address2.setPostNo("1112222");
            address2.setAddress("住所２");

            sut.batchInsert(Arrays.asList(address1, address2));

            assertThat("2レコード登録されていること", sut.findAll(Address.class).size(), is(2));

            Address actual1 = sut.findById(Address.class, 1L, "2");
            assertThat(actual1.getId(), is(1L));
            assertThat(actual1.getCode(), is("2"));
            assertThat(actual1.getUserId(), is(10L));
            assertThat(actual1.getPostNo(), is("1231234"));
            assertThat(actual1.getAddress(), is("住所"));

            Address actual2 = sut.findById(Address.class, 10L, "3");
            assertThat(actual2.getId(), is(10L));
            assertThat(actual2.getCode(), is("3"));
            assertThat(actual2.getUserId(), is(20L));
            assertThat(actual2.getPostNo(), is("1112222"));
            assertThat(actual2.getAddress(), is("住所２"));
        }

        バージョンカラムがNumber互換型ではないテーブル:
        {
            Users2 user1 = new Users2();
            user1.setId(100L);
            user1.setVersion("99");
            user1.setName("name");
            user1.setBirthday(DateUtil.getDate("19001212"));
            user1.setInsertDate(DaoTestHelper.getDate("20001231010101"));

            Users2 user2 = new Users2();
            user2.setId(200L);
            user2.setVersion("999");
            user2.setName("name2");
            user2.setBirthday(DateUtil.getDate("19001213"));
            user2.setInsertDate(DaoTestHelper.getDate("20001231010102"));

            sut.batchInsert(Arrays.asList(user1, user2));

            Users2 actual1 = sut.findById(Users2.class, 100L);
            assertThat(actual1.getId(), is(100L));
            assertThat(actual1.getName(), is("name"));
            assertThat(actual1.getBirthday(), is(DateUtil.getDate("19001212")));
            assertThat(actual1.getInsertDate(), is(DaoTestHelper.getDate("20001231010101")));
            assertThat(actual1.getVersion(), is("99"));

            Users2 actual2 = sut.findById(Users2.class, 200L);
            assertThat(actual2.getId(), is(200L));
            assertThat(actual2.getName(), is("name2"));
            assertThat(actual2.getBirthday(), is(DateUtil.getDate("19001213")));
            assertThat(actual2.getInsertDate(), is(DaoTestHelper.getDate("20001231010102")));
            assertThat(actual2.getVersion(), is("999"));
        }
    }

    /**
     * {@link BasicDaoContext#batchInsert(java.util.List)}のサロゲートペア有のテスト。
     *
     * @throws Exception
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void batchInsert_surrogatePair() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.SEQUENCE, mockSequenceIdGenerator);
        sut.setIdGenerator(GenerationType.TABLE, mockTableIdGenerator);

        new Expectations() {{
            mockSequenceIdGenerator.generateId("USER_ID_SEQ");
            returns("1", "3", "5");
        }};

        バージョンカラムが存在しているテーブル:
        {
            Users user1 = new Users();
            user1.setName("なまえ１");
            user1.setBirthday(DateUtil.getDate("19900101"));
            user1.setInsertDate(DaoTestHelper.getDate("20000101010101"));

            Users user2 = new Users();
            user2.setName("なまえ２\uD840\uDC0B");
            user2.setBirthday(DateUtil.getDate("19900102"));
            user2.setInsertDate(DaoTestHelper.getDate("20000101010102"));

            Users user3 = new Users();
            user3.setName("なまえ３");
            user3.setBirthday(DateUtil.getDate("19900103"));
            user3.setInsertDate(DaoTestHelper.getDate("20000101010103"));

            sut.batchInsert(Arrays.asList(user1, user2, user3));

            assertThat("3レコード登録されていること", sut.findAll(Users.class).size(), is(3));

            Users actual1 = sut.findById(Users.class, 1L);
            assertThat(actual1.getId(), is(1L));
            assertThat(actual1.getName(), is("なまえ１"));
            assertThat(actual1.getBirthday(), is(DateUtil.getDate("19900101")));
            assertThat(actual1.getInsertDate(), is(DaoTestHelper.getDate("20000101010101")));
            assertThat(actual1.getVersion(), is(0L));

            Users actual2 = sut.findById(Users.class, 3L);
            assertThat(actual2.getId(), is(3L));
            assertThat(actual2.getName(), is("なまえ２\uD840\uDC0B"));
            assertThat(actual2.getBirthday(), is(DateUtil.getDate("19900102")));
            assertThat(actual2.getInsertDate(), is(DaoTestHelper.getDate("20000101010102")));
            assertThat(actual2.getVersion(), is(0L));

            Users actual3 = sut.findById(Users.class, 5L);
            assertThat(actual3.getId(), is(5L));
            assertThat(actual3.getName(), is("なまえ３"));
            assertThat(actual3.getBirthday(), is(DateUtil.getDate("19900103")));
            assertThat(actual3.getInsertDate(), is(DaoTestHelper.getDate("20000101010103")));
            assertThat(actual3.getVersion(), is(0L));
        }
    }

    /**
     * シーケンスで採番してbatch insertできること。
     *
     * @throws Exception
     */
    @Test
    public void batchInsertFromSequenceGenerator() throws Exception {
        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.SEQUENCE, mockSequenceIdGenerator);

        Users user1 = new Users(
                null, "name", DateUtil.getDate("20140101"), DaoTestHelper.getDate("20150401123456"));
        Users user2 = new Users(
                null, "name2", DateUtil.getDate("20140102"), DaoTestHelper.getDate("20150402123456"));

        new Expectations() {{
            mockSequenceIdGenerator.generateId("USER_ID_SEQ");
            returns("999", "9999");
        }};

        sut.batchInsert(Arrays.asList(user1, user2));
        assertThat("採番された値が設定されていること[user1]", user1.getId(), is(999L));
        assertThat("採番された値が設定されていること[user2]", user2.getId(), is(9999L));

        Users actual1 = sut.findById(Users.class, user1.getId());
        assertThat(actual1.getId(), is(user1.getId()));
        assertThat(actual1.getName(), is(user1.getName()));
        assertThat(actual1.getBirthday(), is(user1.getBirthday()));
        assertThat(actual1.getInsertDate(), is(user1.getInsertDate()));

        Users actual2 = sut.findById(Users.class, user2.getId());
        assertThat(actual2.getId(), is(user2.getId()));
        assertThat(actual2.getName(), is(user2.getName()));
        assertThat(actual2.getBirthday(), is(user2.getBirthday()));
        assertThat(actual2.getInsertDate(), is(user2.getInsertDate()));
    }

    /**
     * 採番方法でsequenceを指定した場合で、ダイアレクトでサポートされていない場合。
     * <p/>
     * ダイアレクトでサポートされていないので例外が発生する。
     */
    @Test(expected = IllegalEntityException.class)
    public void batchInsertFromSequenceGenerator_Unsupported() throws Exception {
        Users user = new Users(null, "name", DateUtil.getDate("20150115"), DaoTestHelper.getDate("20150401123456"));
        sut.batchInsert(Arrays.asList(user));
    }

    /**
     * テーブルで採番してbatch insertできること
     */
    @Test
    public void batchInsertFromTableGenerator() throws Exception {
        new Expectations() {{
            mockTableIdGenerator.generateId("ADDRESS_ID_SEQ");
            returns("12345", "54321");
        }};
        final Address address1 = new Address(null, "1", 1L, "1111111", "住所1");
        final Address address2 = new Address(null, "2", 2L, "2222222", "住所2");
        sut.batchInsert(Arrays.asList(address1, address2));

        assertThat("採番された値が設定されていること[address1]", address1.getId(), is(12345L));
        assertThat("採番された値が設定されていること[address2]", address2.getId(), is(54321L));

        Address actual1 = sut.findById(Address.class, address1.getId(), "1");
        assertThat(actual1.getId(), is(address1.getId()));
        assertThat(actual1.getCode(), is(address1.code));
        assertThat(actual1.getUserId(), is(address1.getUserId()));
        assertThat(actual1.getPostNo(), is(address1.postNo));
        assertThat(actual1.getAddress(), is(address1.address));

        Address actual2 = sut.findById(Address.class, address2.getId(), "2");
        assertThat(actual2.getId(), is(address2.getId()));
        assertThat(actual2.getCode(), is(address2.code));
        assertThat(actual2.getUserId(), is(address2.getUserId()));
        assertThat(actual2.getPostNo(), is(address2.postNo));
        assertThat(actual2.getAddress(), is(address2.address));

    }

    /**
     * GeneratedValueでidentityの場合で、batch insert後に採番された値が取得できること。
     */
    @Test
    public void batchInsertFromIdentityGenerator() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return true;
            }

            @Override
            public boolean supportsIdentityWithBatchInsert() {
                return true;
            }
        });

        new Expectations(connection) {{
            SqlPStatement statement = connection.prepareStatement(anyString, (String[]) withNotNull());
            ResultSet keys = statement.getGeneratedKeys();
            keys.next();
            returns(true, true, true, false);
            keys.getString(1);
            returns("1", "10", "100");
        }};
        sut.setDbConnection(connection);
        IdentityGenUsers user1 = new IdentityGenUsers(
                null, "name1", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));
        IdentityGenUsers user2 = new IdentityGenUsers(
                null, "name2", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));
        IdentityGenUsers user3 = new IdentityGenUsers(
                null, "name3", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));

        sut.batchInsert(Arrays.asList(user1, user2, user3));
        assertThat(user1.getId(), is(1L));
        assertThat(user2.getId(), is(10L));
        assertThat(user3.getId(), is(100L));
    }

    /**
     * GeneratedValueでidentityの場合で、ResultSet#close時の例外は無視されること
     * (採番された値が取得できること。)
     */
    @Test
    public void batchInsertFromIdentityGenerator_ResultSetCloseError() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return true;
            }

            @Override
            public boolean supportsIdentityWithBatchInsert() {
                return true;
            }
        });

        new Expectations(connection) {{
            SqlPStatement statement = connection.prepareStatement(anyString, (String[]) withNotNull());
            ResultSet keys = statement.getGeneratedKeys();
            keys.next();
            returns(true, false);
            keys.getString(1);
            result = "54321";
            keys.close();
            result = new SQLException("error");
        }};
        sut.setDbConnection(connection);
        AutoGenUsers user = new AutoGenUsers(
                null, "name", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));

        sut.batchInsert(Collections.singletonList(user));
        assertThat(user.getId(), is(54321L));
    }

    /**
     * GeneratedValueでidentityの場合で、採番された値取得時にDB例外が発生した場合
     */
    @Test
    public void batchInsertFromIdentity_getGeneratedKeyError() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return true;
            }

            @Override
            public boolean supportsIdentityWithBatchInsert() {
                return true;
            }
        });

        new Expectations(connection) {{
            SqlPStatement statement = connection.prepareStatement(anyString, (String[]) withNotNull());
            statement.getGeneratedKeys().next();
            result = new SQLException("error");
        }};
        sut.setDbConnection(connection);
        IdentityGenUsers user = new IdentityGenUsers(
                null, "name", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));

        RuntimeException exception = null;
        try {
            sut.batchInsert(Collections.singletonList(user));
        } catch (RuntimeException e) {
            exception = e;
        }
        assertThat(exception, is(instanceOf(DbAccessException.class)));
        assertThat(exception.getMessage(), containsString("failed to get auto generated key."));
    }

    /**
     * GeneratedValueでidentityの場合で、採番された値が取得出来ない場合。
     */
    @Test
    public void batchInsertFromIdentity_getGeneratedKeyNotFound() throws Exception {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return true;
            }

            @Override
            public boolean supportsIdentityWithBatchInsert() {
                return true;
            }
        });

        new Expectations(connection) {{
            SqlPStatement statement = connection.prepareStatement(anyString, (String[]) withNotNull());
            final ResultSet rs = statement.getGeneratedKeys();
            rs.next();
            returns(true, false);
            rs.getString(1);
            returns("100", "200");

        }};
        sut.setDbConnection(connection);
        IdentityGenUsers user1 = new IdentityGenUsers(
                null, "name1", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));
        IdentityGenUsers user2 = new IdentityGenUsers(
                null, "name2", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));

        RuntimeException exception = null;
        try {
            sut.batchInsert(Arrays.asList(user1, user2));
        } catch (RuntimeException e) {
            exception = e;
        }
        assertThat(exception, is(instanceOf(IllegalStateException.class)));
        assertThat(exception.getMessage(), containsString("generated key not found."));
    }

    /**
     * 採番方法でidentityを指定した場合で、ダイアレクトでサポートされていない場合。
     * <p/>
     * ダイアレクトでサポートされていないので例外が発生する。
     */
    @Test(expected = IllegalEntityException.class)
    public void batchInsertFromIdentity_unsupportedDialect() {

        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return false;
            }
        });
        sut.setDbConnection(connection);
        IdentityGenUsers user = new IdentityGenUsers(
                null, "name", DateUtil.getDate("19990102"), DaoTestHelper.getDate("20150401123456"));

        sut.batchInsert(Collections.singletonList(user));
    }

    /**
     * 採番方法でautoを指定した場合でシーケンスをサポートするDialectの場合。
     * <p/>
     * シーケンスを使った採番処理が行われること。
     * なお、シーケンス名は「テーブル名 + '_' + カラム名」であること
     */
    @Test
    public void batchInsertFromAuto_sequenceSupportDialect() throws Exception {
        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.SEQUENCE, mockSequenceIdGenerator);

        new Expectations() {{
            mockSequenceIdGenerator.generateId("DAO_USERS_USER_ID");
            returns("100", "200");
        }};

        AutoGenUsers user1 = new AutoGenUsers(null, "name2", DateUtil.getDate("20150115"),
                DaoTestHelper.getDate("20150401123456"));
        AutoGenUsers user2 = new AutoGenUsers(null, "name2", DateUtil.getDate("20150115"),
                DaoTestHelper.getDate("20150401123456"));
        sut.batchInsert(Arrays.asList(user1, user2));

        AutoGenUsers actual = sut.findById(AutoGenUsers.class, user1.getId());
        assertThat(actual.getName(), is(user1.getName()));
        assertThat(actual.getBirthday(), is(user1.getBirthday()));
        assertThat("採番された値が設定されていること", user1.getId(), is(100L));

        assertThat("2番めのEntityにも採番した値が設定されていること", user2.getId(), is(200L));

    }

    /**
     * 採番方法でautoを指定した場合でシーケンスもIDENTITYもサポートしないダイアレクトの場合。
     * <p/>
     * テーブルを使った採番処理が行われること。 なお、テーブル採番の識別子は「テーブル名 + '_' + カラム名」であること
     */
    @Test
    public void batchInsertFromAuto_tableSupportDialect() throws Exception {
        sut = new BasicDaoContext(new StandardSqlBuilder(), new DefaultDialect() {
            @Override
            public boolean supportsIdentity() {
                return false;
            }

            @Override
            public boolean supportsSequence() {
                return false;
            }
        });
        sut.setDbConnection(connection);
        sut.setIdGenerator(GenerationType.TABLE, mockTableIdGenerator);

        new Expectations() {{
            mockTableIdGenerator.generateId("DAO_USERS_USER_ID");
            returns("101", "102");
        }};

        AutoGenUsers users1 = new AutoGenUsers(null, "1", DateUtil.getDate("20000102"),
                DaoTestHelper.getDate("20150401123456"));
        AutoGenUsers users2 = new AutoGenUsers(null, "2", DateUtil.getDate("20000102"),
                DaoTestHelper.getDate("20150401123456"));

        sut.batchInsert(Arrays.asList(users1, users2));

        assertThat("採番された値が設定されていること", users1.getId(), is(101L));
        AutoGenUsers actual = sut.findById(AutoGenUsers.class, users1.getId());
        assertThat(actual.getId(), is(users1.getId()));
        assertThat(actual.getName(), is(users1.getName()));
        assertThat(actual.getBirthday(), is(users1.getBirthday()));
        assertThat(actual.getInsertDate(), is(users1.getInsertDate()));

        assertThat("2番めのEntityにも採番された値が設定されていること", users2.getId(), is(102L));
    }

    /**
     * 一括登録で空のリストを指定した場合、処理は正常に終了すること。（処理はなにも実行されない。)
     */
    @Test
    public void batchInsert_emptyList() throws Exception {

        VariousDbTestHelper.delete(Users.class);

        sut.setDbConnection(connection);
        sut.batchInsert(new ArrayList<Users>());

        assertThat("テスト実行後もテーブルは空のまま",
                VariousDbTestHelper.findAll(Users.class), empty());

    }

    /**
     * {@link BasicDaoContext#delete(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {

        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 99L),
                new Users(2L, "name_2", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(3L, "name_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"), 99L));

        主キーが単一のテーブル:
        {

            assertThat("削除対象のデータが存在していること。", sut.findById(Users.class, 3L), is(notNullValue()));

            Users user = new Users();
            user.setId(3L);
            sut.delete(user);

            try {
                sut.findById(Users.class, 3L);
                fail("データが削除されたのでここは通らない。");
            } catch (NoDataException e) {
                // assertする必要ないが、一応。。。
                assertThat("データが削除されたのでNoDataExceptionが発生する", e, is(instanceOf(NoDataException.class)));
            }
        }

        主キーが複数のテーブル:
        {
            VariousDbTestHelper.setUpTable(
                    new Address(100L, "1", 1L, "1001001", "いえ"),
                    new Address(100L, "2", 1L, "1001002", "かいしゃ")
            );

            Address address = new Address();
            address.setId(100L);
            address.setCode("2");
            int actual = sut.delete(address);

            assertThat("削除されたレコードは1", actual, is(1));
            EntityList<Address> addresses = sut.findAll(Address.class);
            assertThat("1レコード削除されていること", addresses.size(), is(1));
            assertThat("code=1のレコードは削除対象外なので残っていること", addresses.get(0)
                    .getCode(), is("1"));
        }

        更新対象が存在しない場合:
        {

            Address address = new Address();
            address.setId(100L);
            address.setCode("3");
            int actual = sut.delete(address);
            assertThat("更新対象が存在しないので結果は0", actual, is(0));
        }
    }

    /**
     * {@link BasicDaoContext#batchDelete(java.util.List)} のテスト。
     *
     * @throws Exception
     */
    @Test
    public void batchDelete() throws Exception {

        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 99L),
                new Users(2L, "name_2", DateUtil.getDate("20120102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(3L, "name_3", DateUtil.getDate("20120103"), DaoTestHelper.getDate("20150403123456"), 99L));

        主キーが単一のテーブル:
        {

            sut.batchDelete(Arrays.asList(new Users(1L), new Users(3L)));

            final Users user2 = sut.findById(Users.class, 2L);
            assertThat("削除されていないので取得できる", user2, is(notNullValue()));
            try {
                sut.findById(Users.class, 1L);
                fail("データが削除されたのでここは通らない。");
            } catch (NoDataException e) {
                // assertする必要ないが、一応。。。
                assertThat("データが削除されたのでNoDataExceptionが発生する", e, is(instanceOf(NoDataException.class)));
            }
            try {
                sut.findById(Users.class, 3L);
                fail("データが削除されたのでここは通らない。");
            } catch (NoDataException e) {
                // assertする必要ないが、一応。。。
                assertThat("データが削除されたのでNoDataExceptionが発生する", e, is(instanceOf(NoDataException.class)));
            }
        }

        主キーが複数のテーブル:
        {
            VariousDbTestHelper.setUpTable(
                    new Address(100L, "1", 1L, "1001001", "いえ"),
                    new Address(100L, "2", 1L, "1001002", "かいしゃ"),
                    new Address(101L, "1", 1L, "1001002", "いえ")
            );

            final Address address1 = new Address();
            address1.setId(100L);
            address1.setCode("2");

            final Address address2 = new Address();
            address2.setId(101L);
            address2.setCode("1");
            sut.batchDelete(Arrays.asList(address1, address2));

            EntityList<Address> addresses = sut.findAll(Address.class);
            assertThat("1レコードだけ削除されずに残る", addresses.size(), is(1));
            assertThat("id=100のレコードは削除対象外なので残っていること", addresses.get(0).getId(), is(100L));
            assertThat("code=1のレコードは削除対象外なので残っていること", addresses.get(0).getCode(), is("1"));
        }
    }

    /**
     * 一括削除で空のリストを指定した場合、処理は正常に終了すること。（処理はなにも実行されない。)
     */
    @Test
    public void batchDelete_emptyList() throws Exception {
        final Users user1 = new Users(
                1L, "name_1", DateUtil.getDate("20120101"), DaoTestHelper.getDate("20150401123456"), 99L);
        final Users user2 = new Users(2L, "name_2", DateUtil.getDate("20120102"),
                DaoTestHelper.getDate("20150402123456"), 99L);
        VariousDbTestHelper.setUpTable(user1, user2);

        sut.batchDelete(new ArrayList<Users>());

        final List<Users> users = VariousDbTestHelper.findAll(Users.class, "id");
        assertThat("削除されていないこと", users, contains(user1, user2));
    }

    /**
     * {@link BasicDaoContext#tableName(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void tableName() throws Exception {
        assertThat("テーブル名が取得できる", sut.tableName(new Users()), is("DAO_USERS"));
        assertThat("Tableアノテーションのname属性がテーブル名となる", sut.tableName(new Address()), is("USER_ADDRESS"));

        try {
            sut.tableName(new Object());
            fail("ここは通らない");
        } catch (Exception e) {
            assertThat("Entityクラスではないのでエラーが発生すること", e, is(instanceOf(IllegalEntityException.class)));
        }
    }
}

