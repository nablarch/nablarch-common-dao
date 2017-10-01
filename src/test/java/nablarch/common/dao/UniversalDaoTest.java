package nablarch.common.dao;

import static nablarch.common.dao.UniversalDao.exists;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.common.dao.DaoTestHelper.Address;
import nablarch.common.dao.DaoTestHelper.Users;
import nablarch.common.dao.DaoTestHelper.SqlFunctionResult;
import nablarch.common.dao.UniversalDao.Transaction;
import nablarch.common.idgenerator.IdGenerator;
import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.connection.BasicDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.ConnectionFactorySupport;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.transaction.TransactionFactory;
import nablarch.core.util.DateUtil;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Mocked;

/**
 * @author kawasima
 */
@RunWith(DatabaseTestRunner.class)
public class UniversalDaoTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /** テスト用データベース接続 */
    private TransactionManagerConnection connection;

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(Users.class);
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

    /**
     * {@link UniversalDao#findById(Class, Object...)}のテスト。
     * <p/>
     * 主キーのカラムが1つだけの場合のケース
     */
    @Test
    public void findById_singlePrimaryKey() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20140101"), DaoTestHelper.getDate("20150401123456"), 9L, false),
                new Users(2L, "name_2", DateUtil.getDate("20140102"), DaoTestHelper.getDate("20150402123456"), 99L, true),
                new Users(3L, "name_3", DateUtil.getDate("20140103"), DaoTestHelper.getDate("20150403123456"), 999L, false)
        );
        Users user = UniversalDao.findById(Users.class, 2L);
        assertThat(user.getId(), is(2L));
        assertThat(user.getName(), is("name_2"));
        assertThat(user.getBirthday(), is(DateUtil.getDate("20140102")));
        assertThat(user.getInsertDate(), is(DaoTestHelper.getDate("20150402123456")));
        assertThat(user.getVersion(), is(99L));
        assertThat(user.isActive(), is(true));
    }

    /**
     * {@link UniversalDao#findById(Class, Object...)}のテスト。
     * <p/>
     * 主キーのカラムが複数の場合のケース
     */
    @Test
    public void findById_multiplePrimaryKey() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Address(1L, "1", 10L, "1111111", "住所")
        );

        Address address = UniversalDao.findById(Address.class, 1L, "1");
        assertThat(address.getId(), is(1L));
        assertThat(address.getCode(), is("1"));
        assertThat(address.getUserId(), is(10L));
        assertThat(address.getPostNo(), is("1111111"));
        assertThat(address.getAddress(), is("住所"));
    }

    /**
     * {@link UniversalDao#findById(Class, Object...)}のテスト。
     * <p/>
     * 複合主キーの場合で、主キー検索未対応の場合（データベースのメタ情報が取れなかった場合）
     */
    @Test
    public void findById_unsupported() throws Exception {

        // 主キー検索可否をfalseで上書き
        EntityMeta entityMeta = EntityUtil.findEntityMeta(Address.class);

        Deencapsulation.setField(entityMeta, "enableFindById", false);
        try {
            UniversalDao.findById(Address.class, 1L, "1");
            fail("ここはとおらない");
        } catch (Exception e) {
            assertThat(e, is(instanceOf(IllegalStateException.class)));
        } finally {
            // 強制的に書き換えを行ったのでキャッシュを綺麗にする。
            EntityUtil.clearCache();
        }
    }

    /**
     * {@link UniversalDao#findAll(Class)}のテストケース
     *
     * @throws Exception
     */
    @Test
    public void findAll() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456")),
                new Users(2L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456")),
                new Users(3L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456")),
                new Users(4L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456")),
                new Users(5L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456"))
        );
        EntityList<Users> allUsers = UniversalDao.findAll(Users.class);
        assertThat("全レコード取得できること", allUsers.size(), is(5));

        List<Users> result = new ArrayList<Users>(allUsers);

        // idでソートする
        Collections.sort(result, new Comparator<Users>() {
            @Override
            public int compare(Users o1, Users o2) {
                if (o1.getId() > o2.getId()) {
                    return 1;
                } else if (o1.getId() < o2.getId()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });

        long id = 1;
        for (Users user : result) {
            assertThat(user.getId(), is(id));
            id++;
        }
    }
    
    /**
     * {@link UniversalDao#findAllBySqlFile(Class, String, Object)}のテストケース
     *
     * @throws Exception
     */
    @Test
    public void findAllBySqlFile_Condition() throws Exception {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 20; i++) {
            long index = i + 1;
            VariousDbTestHelper.insert(new Users(index, "name_" + index, new Date(), DaoTestHelper.getDate("20150401123456")));
        }

        Users cond = new Users();
        cond.setName("name_2");

        EntityList<Users> users = UniversalDao.findAllBySqlFile(Users.class,
                "FIND_USERS_ALL_WHERE_ENTITY", cond);
        assertThat(users.size(), is(2));
    }

    /**
     * {@link UniversalDao#findAllBySqlFile(Class, String)}のテストケース
     *
     * @throws Exception
     */
    @Test
    public void findAllBySqlFile_NoCondition() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(10L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456")),
                new Users(11L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456")),
                new Users(12L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456")),
                new Users(13L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456")),
                new Users(14L, "name", DateUtil.getDate("20000101"), DaoTestHelper.getDate("20150401123456"))
        );

        EntityList<Users> allUsers = UniversalDao.findAllBySqlFile(
                Users.class, "FIND_ALL_USERS");

        assertThat("全て取得できること", allUsers.size(), is(5));

        List<Users> result = new ArrayList<Users>(allUsers);
        // idでソートする
        Collections.sort(result, new Comparator<Users>() {
            @Override
            public int compare(Users o1, Users o2) {
                if (o1.getId() > o2.getId()) {
                    return 1;
                } else if (o1.getId() < o2.getId()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });

        long id = 10;

        for (Users user : allUsers) {
            assertThat(user.getId(), is(id));
            id++;
        }
    }
    
    /**
     * {@link UniversalDao#findBySqlFile(Class, String, Object)}のテストケース
     *
     * @throws Exception
     */
    @Test
    public void findBySqlFile() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", DateUtil.getDate("20110101"), DaoTestHelper.getDate("20150401123456"), 9L),
                new Users(2L, "name_2", DateUtil.getDate("20110102"), DaoTestHelper.getDate("20150402123456"), 99L),
                new Users(3L, "name_3", DateUtil.getDate("20110103"), DaoTestHelper.getDate("20150403123456"), 999L)
        );

        Users cond = new Users();
        cond.setId(3L);

        Users user = UniversalDao.findBySqlFile(Users.class, "FIND_BY_ID", cond);
        assertThat(user.getId(), is(3L));
        assertThat(user.getName(), is("name_3"));
        assertThat(user.getBirthday(), is(DateUtil.getDate("20110103")));
        assertThat(user.getVersion(), is(999L));
    }
    
    /**
     * {@link UniversalDao#countBySqlFile(Class, String)}のテストケース
     *
     * @throws Exception
     */
    @Test
    public void countBySqlFile_NoCondition() throws Exception {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 20; i++) {
            VariousDbTestHelper.insert(new Users((long) (i + 1)));
        }
        long actual = UniversalDao.countBySqlFile(Users.class, "FIND_ALL_USERS");
        assertThat(actual, is(20L));
    }

    /**
     * {@link UniversalDao#countBySqlFile(Class, String, Object)}のテストケース。
     *
     * @throws Exception
     */
    @Test
    public void countBySqlFile_Condition() throws Exception {
        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 20; i++) {
            long index = i + 1;
            VariousDbTestHelper.insert(new Users(index, "name_" + index, DateUtil.getDate(String.valueOf(
                    20150100 + index)), DaoTestHelper.getDate("20150401123456")));
        }

        Users cond = new Users();
        cond.setName("name_2");
        long actual = UniversalDao.countBySqlFile(Users.class, "FIND_USERS_ALL_WHERE_ENTITY", cond);

        assertThat(actual, is(2L));
    }

    /**
     * {@link UniversalDao#exists(Class, String)}のテストケース
     *
     * @throws Exception
     */
    @Test
    public void exists_NoCondition() throws Exception {
        VariousDbTestHelper.delete(Users.class);
        assertThat("データが無いのでfalse",
                exists(Users.class, "FIND_ALL_USERS"), is(false));

        for (int i = 0; i < 10; i++) {
            VariousDbTestHelper.insert(new Users((long) (i + 1)));
        }
        assertThat("データが作成されたのでtrue",
                exists(Users.class, "FIND_ALL_USERS"), is(true));
    }

    /**
     * {@link UniversalDao#exists(Class, String, Object)}のテストケース。
     *
     * @throws Exception
     */
    @Test
    public void exists_Condition() throws Exception {

        VariousDbTestHelper.delete(Users.class);
        Users cond = new Users();
        cond.setName("name_2");

        assertThat("データが無いのでfalse", exists(Users.class, "FIND_USERS_ALL_WHERE_ENTITY", cond), is(false));

        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", new Date(), DaoTestHelper.getDate("20150401123456")),
                new Users(2L, "name_2", new Date(), DaoTestHelper.getDate("20150401123456"))
        );

        assertThat("データが作成されたのでtrue", exists(Users.class, "FIND_USERS_ALL_WHERE_ENTITY", cond), is(true));
    }

    /**
     * {@link UniversalDao#update(Object)}のテストケース。
     */
    @Test
    public void update() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", new Date(), DaoTestHelper.getDate("20150401123456"), 1L),
                new Users(2L, "name_2", new Date(), DaoTestHelper.getDate("20150401123456"), 2L),
                new Users(3L, "name_3", new Date(), DaoTestHelper.getDate("20150401123456"), 3L)
        );

        Users user = UniversalDao.findById(Users.class, 2L);
        user.setName("なまえに更新");

        int updateCount = UniversalDao.update(user);

        assertThat("更新されたレコードは1", updateCount, is(1));

        Users actual = UniversalDao.findById(Users.class, 2L);
        assertThat(actual.getId(), is(user.getId()));
        assertThat(actual.getName(), is(user.getName()));
        assertThat(actual.getBirthday(), is(user.getBirthday()));
        assertThat(actual.getInsertDate(), is(user.getInsertDate()));
        assertThat(actual.getVersion(), is(user.getVersion() + 1));
    }

    /**
     * {@link UniversalDao#batchUpdate}のテスト。
     */
    @Test
    public void batchUpdate() throws Exception {
        VariousDbTestHelper.setUpTable(
                new Users(1L, "name_1", new Date(), DaoTestHelper.getDate("20150401123456"), 1L),
                new Users(2L, "name_2", new Date(), DaoTestHelper.getDate("20150401123456"), 2L),
                new Users(3L, "name_3", new Date(), DaoTestHelper.getDate("20150401123456"), 3L)
        );

        Users user1 = UniversalDao.findById(Users.class, 1L);
        user1.setName("なまえに更新1");

        Users user2 = UniversalDao.findById(Users.class, 2L);
        user2.setName("なまえに更新2");

        Users user3 = UniversalDao.findById(Users.class, 3L);
        user3.setName("なまえに更新3(バージョン番号が不一致)");
        user3.setVersion(user3.getVersion() + 1);

        UniversalDao.batchUpdate(Arrays.asList(user1, user2, user3));

        Users actual1 = UniversalDao.findById(Users.class, 1L);
        assertThat(actual1.getId(), is(user1.getId()));
        assertThat(actual1.getName(), is(user1.getName()));
        assertThat(actual1.getBirthday(), is(user1.getBirthday()));
        assertThat(actual1.getInsertDate(), is(user1.getInsertDate()));
        assertThat(actual1.getVersion(), is(user1.getVersion() + 1));

        Users actual2 = UniversalDao.findById(Users.class, 2L);
        assertThat(actual2.getId(), is(user2.getId()));
        assertThat(actual2.getName(), is(user2.getName()));
        assertThat(actual2.getBirthday(), is(user2.getBirthday()));
        assertThat(actual2.getInsertDate(), is(user2.getInsertDate()));
        assertThat(actual2.getVersion(), is(user2.getVersion() + 1));

        Users actual3 = UniversalDao.findById(Users.class, 3L);
        assertThat(actual3.getId(), is(user3.getId()));
        assertThat("更新されない", actual3.getName(), is("name_3"));
        assertThat(actual3.getBirthday(), is(user3.getBirthday()));
        assertThat(actual3.getInsertDate(), is(user3.getInsertDate()));
        assertThat("バージョンはインクリメントされない", actual3.getVersion(), is(3L));
    }

    /**
     * {@link UniversalDao#insert(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void insert(@Mocked final IdGenerator mockGenerator) throws Exception {
        VariousDbTestHelper.delete(Users.class);

        new Expectations() {{
            mockGenerator.generateId("USER_ID_SEQ");
            result = "1";
        }};

        Users user = new Users();
        user.setName("ユーザ名");
        user.setBirthday(DateUtil.getDate("19700101"));
        user.setInsertDate(DaoTestHelper.getDate("20150401123456"));

        final DaoContextFactory daoContextFactory = new BasicDaoContextFactory();
        setDialect(new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        }, connection);
        daoContextFactory.setSequenceIdGenerator(mockGenerator);
        repositoryResource.addComponent("daoContextFactory", daoContextFactory);

        UniversalDao.insert(user);
        
        connection.commit();
        
        Users actual = VariousDbTestHelper.findById(Users.class, 1L);
        
        assertThat(actual.getId(), is(user.getId()));
        assertThat(actual.getName(), is(user.getName()));
        assertThat(actual.getBirthday(), is(user.getBirthday()));
        assertThat(actual.getInsertDate(), is(user.getInsertDate()));
        assertThat(actual.getVersion(), is(user.getVersion()));
    }

    /**
     * {@link UniversalDao#batchInsert(List)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void batchInsert(@Mocked final IdGenerator mockGenerator) throws Exception {
        VariousDbTestHelper.delete(Users.class);

        new Expectations() {{
            mockGenerator.generateId("USER_ID_SEQ");
            returns("1", "100");
        }};

        Users user1 = new Users(
                null, "ユーザ名1", DateUtil.getDate("19700101"), DaoTestHelper.getDate("20150401123456"));
        Users user2 = new Users(
                null, "ユーザ名2", DateUtil.getDate("19700101"), DaoTestHelper.getDate("20150401123456"));

        final DaoContextFactory daoContextFactory = new BasicDaoContextFactory();
        setDialect(new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        }, connection);
        daoContextFactory.setSequenceIdGenerator(mockGenerator);
        repositoryResource.addComponent("daoContextFactory", daoContextFactory);

        UniversalDao.batchInsert(Arrays.asList(user1, user2));
        connection.commit();

        Users actual1 = VariousDbTestHelper.findById(Users.class, 1L);
        assertThat(actual1.getId(), is(user1.getId()));
        assertThat(actual1.getName(), is(user1.getName()));
        assertThat(actual1.getBirthday(), is(user1.getBirthday()));
        assertThat(actual1.getInsertDate(), is(user1.getInsertDate()));
        assertThat(actual1.getVersion(), is(user1.getVersion()));

        Users actual2 = VariousDbTestHelper.findById(Users.class, 100L);
        assertThat(actual2.getId(), is(user2.getId()));
        assertThat(actual2.getName(), is(user2.getName()));
        assertThat(actual2.getBirthday(), is(user2.getBirthday()));
        assertThat(actual2.getInsertDate(), is(user2.getInsertDate()));
        assertThat(actual2.getVersion(), is(user2.getVersion()));
    }

    /**
     * {@link UniversalDao#delete(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {

        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 10; i++) {
            VariousDbTestHelper.insert(new Users((long) (i + 1)));
        }

        assertThat("削除前は10レコードある", UniversalDao.findAll(Users.class)
                .size(), is(10));
        int count = 10;
        for (int i = 1; i <= 10; i++) {
            count--;
            Users users = new Users();
            users.setId((long) i);
            int deleteCount = UniversalDao.delete(users);
            assertThat(deleteCount, is(1));
            assertThat(UniversalDao.findAll(Users.class)
                    .size(), is(count));
        }
    }

    /**
     * {@link UniversalDao#batchDelete(List)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void batchDelete() throws Exception {

        VariousDbTestHelper.delete(Users.class);
        List<Users> users = new ArrayList<Users>();
        for (int i = 0; i < 10; i++) {
            final Users user = new Users((long) (i + 1));
            VariousDbTestHelper.insert(user);
            users.add(user);
        }

        assertThat("削除前は10レコードある", UniversalDao.findAll(Users.class).size(), is(10));
        UniversalDao.batchDelete(users);

        assertThat("全て削除されたのでレコードは存在しない", UniversalDao.findAll(Users.class).size(), is(0));
    }

    /**
     * ページングのテスト
     */
    @Test
    public void paging() {

        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 20; i++) {
            VariousDbTestHelper.insert(new Users((long) (i + 1)));
        }
        EntityList<Users> page1 = UniversalDao.per(3)
                .page(1)
                .findAllBySqlFile(Users.class, "FIND_ALL_USERS");
        assertThat(page1.size(), is(3));
        assertThat(page1.get(0)
                .getId(), is(1L));
        assertThat(page1.get(1)
                .getId(), is(2L));
        assertThat(page1.get(2)
                .getId(), is(3L));

        EntityList<Users> page2 = UniversalDao.page(2)
                .per(3)
                .findAllBySqlFile(Users.class, "FIND_ALL_USERS");
        assertThat(page2.size(), is(3));
        assertThat(page2.get(0)
                .getId(), is(4L));
        assertThat(page2.get(1)
                .getId(), is(5L));
        assertThat(page2.get(2)
                .getId(), is(6L));
    }

    /**
     * 遅延ロードのテスト
     */
    @Test
    public void defer() {

        VariousDbTestHelper.delete(Users.class);
        for (int i = 0; i < 10; i++) {
            VariousDbTestHelper.insert(new Users((long) (i + 1)));
        }
        EntityList<Users> users = UniversalDao.defer()
                .findAllBySqlFile(Users.class, "FIND_ALL_USERS");
        assertThat(users, is(instanceOf(DeferredEntityList.class)));

        long id = 1;
        for (Users user : users) {
            assertThat(user.getId() + ":" + user.getName(), user.getId(), is(id));
            id++;
        }
    }

    /**
     * {@link Transaction}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void transaction(@Mocked final IdGenerator mockIdGenerator) throws Exception {
        VariousDbTestHelper.delete(Users.class);

        new Expectations() {{
            mockIdGenerator.generateId("USER_ID_SEQ");
            returns("1", "2");
        }};

        final DaoContextFactory daoContextFactory = new BasicDaoContextFactory();
        daoContextFactory.setSequenceIdGenerator(mockIdGenerator);
        repositoryResource.addComponent("daoContextFactory", daoContextFactory);

        ConnectionFactory connectionFactory = repositoryResource.getComponent("connectionFactory");
        TransactionFactory transactionFactory = repositoryResource.getComponent("jdbcTransactionFactory");

        assertThat("データが無いので結果は0", UniversalDao.findAll(Users.class)
                .size(), is(0));

        final SimpleDbTransactionManager transactionManager = new SimpleDbTransactionManager();
        transactionManager.setDbTransactionName("hoge");
        setDialect(new DefaultDialect() {
            @Override
            public boolean supportsSequence() {
                return true;
            }
        }, connectionFactory); // connectionFactory経由でDialectを取得する。
        transactionManager.setConnectionFactory(connectionFactory);
        transactionManager.setTransactionFactory(transactionFactory);

        class Hoge extends Transaction {

            public Hoge(SimpleDbTransactionManager transactionManager) {
                super(transactionManager);
            }

            public Hoge(String transactionManagerName) {
                super(transactionManagerName);
            }

            @Override
            protected void execute() {
                Users user = new Users();
                user.setName("hoge");
                user.setBirthday(DateUtil.getDate("20140101"));
                UniversalDao.insert(user);
            }
        }
        new Hoge(transactionManager);
        repositoryResource.addComponent("hoge", transactionManager);
        connection.rollback();

        assertThat("別トランザクションでデータが追加されていること",
                UniversalDao.findAll(Users.class)
                        .size(), is(1));

        new Hoge("hoge");
        connection.rollback();

        assertThat("別トランザクションでデータがさらに追加されていること",
                UniversalDao.findAll(Users.class)
                        .size(), is(2));

        repositoryResource.addComponent("daoContextFactory", null);
        try {
            new Hoge("hoge");
            fail("ここはとおらない");
        } catch (Exception e) {
            assertThat("DaoContextFactoryがリポジトリにない場合はエラー", e, is(instanceOf(IllegalStateException.class)));
        }
    }

    /**
     * 集約関数を使ったSQLを使用するテスト(BigDecimal）
     */
    @Test
    public void testUseSqlFunctionBigDecimal() {
        VariousDbTestHelper.delete(Users.class);
        VariousDbTestHelper.setUpTable(
                new Users(1L, null, null, null, null),
                new Users(2L, null, null, null, null)
        );

        List<SqlFunctionResult> actual = UniversalDao.findAllBySqlFile(SqlFunctionResult.class, "USE_FUNCTION_BIG_DECIMAL");
        assertThat(actual.get(0).getBigDecimalCol(), is(new BigDecimal("2.2")));
    }

    /**
     * 集約関数を使ったSQLを使用するテスト(Integer）
     */
    @Test
    public void testUseSqlFunctionInteger() {
        VariousDbTestHelper.delete(Users.class);
        VariousDbTestHelper.setUpTable(
                new Users(1L, null, null, null, null),
                new Users(2L, null, null, null, null)
        );

        List<SqlFunctionResult> actual = UniversalDao.findAllBySqlFile(SqlFunctionResult.class, "USE_FUNCTION_INTEGER");
        assertThat(actual.get(0).getIntegerCol(), is(200));
    }

    /**
     * 集約関数を使ったSQLを使用するテスト(Long）
     */
    @Test
    public void testUseSqlFunctionLong() {
        VariousDbTestHelper.delete(Users.class);
        VariousDbTestHelper.setUpTable(
                new Users(1L, null, null, null, 100000000000000000L), //18桁
                new Users(2L, null, null, null, 900000000000000000L) //18桁
        );

        List<SqlFunctionResult> actual = UniversalDao.findAllBySqlFile(SqlFunctionResult.class, "USE_FUNCTION_LONG");
        assertThat(actual.get(0).getLongCol(), is(1000000000000000000L)); //19桁
    }
    
    @Entity
    @Table(name = "clob_column")
    public static class ClobColumn {
        @Id
        @Column(name = "id", length = 18)
        public Long id;

        @Column(name = "clob", columnDefinition = "clob")
        public String clob;

        @Id
        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getClob() {
            return clob;
        }

        public void setClob(final String clob) {
            this.clob = clob;
        }
    }

    /**
     * CLOB型のカラムにデータを登録できること
     */
    @Test
    public void test_insertClobColumn() throws Exception {
        VariousDbTestHelper.createTable(ClobColumn.class);
        final ClobColumn entity = new ClobColumn();
        entity.id = 1L;
        entity.clob = "clobカラムの値";
        UniversalDao.insert(entity);
        connection.commit();

        final ClobColumn actual = VariousDbTestHelper.findById(ClobColumn.class, entity.id);
        assertThat(actual.clob, is(entity.clob));
    }

    /**
     * CLOB型のカラムのデータを更新できること
     */
    @Test
    public void test_updateClobColumn() throws Exception {
        VariousDbTestHelper.createTable(ClobColumn.class);
        final ClobColumn entity = new ClobColumn();
        entity.id = 12345L;
        entity.clob = "変更前";
        VariousDbTestHelper.insert(entity);
        
        entity.clob = "updateを使って更新";
        UniversalDao.update(entity);
        connection.commit();
        
        final ClobColumn actual = VariousDbTestHelper.findById(ClobColumn.class, entity.id);
        assertThat(actual.clob, is(entity.clob));
    }

    /**
     * Dialectを設定する。
     *
     * @param dialect
     * @param connectionFactory
     */
    private void setDialect(DefaultDialect dialect,
            ConnectionFactory connectionFactory) {
        if (connectionFactory instanceof ConnectionFactorySupport) {
            ConnectionFactorySupport.class.cast(connectionFactory).setDialect(dialect);
            return;
        }
        throw new RuntimeException("can't set dialect to ConnectionFactory. please fix #setDialect method.");
    }

    /**
     * Dialectを設定する。
     *
     * @param dialect
     * @param connection
     */
    private void setDialect(DefaultDialect dialect, TransactionManagerConnection connection) {
        if (connection  instanceof BasicDbConnection) {
            DbExecutionContext context = new DbExecutionContext(connection, dialect, TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
            BasicDbConnection.class.cast(connection).setContext(context);
            return;
        }
        throw new RuntimeException("can't set dialect to TransactionmanagerConnection. please fix #setDialect method.");
    }
}

