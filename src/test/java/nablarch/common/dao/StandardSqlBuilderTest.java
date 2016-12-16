package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.util.StringUtil;

import nablarch.test.support.SystemRepositoryResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * {@link StandardSqlBuilder}のテストクラス。
 */
public class StandardSqlBuilderTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    @Before
    public void setUp() throws Exception {
        repositoryResource.addComponent("databaseMetaDataExtractor", new DaoTestHelper.MockExtractor());
    }

    @After
    public void tearDown() throws Exception {
        DbConnectionContext.removeConnection();
    }

    /** テスト対象 */
    private StandardSqlBuilder sut = new StandardSqlBuilder();

    /**
     * 単一のIDを条件とするSELECT文が構築できること。
     *
     * @throws Exception
     */
    @Test
    public void testBuildSelectByIdFromSingleId() throws Exception {
        assertThat("単一のIDのEntity",
                sut.buildSelectByIdSql(UsersEntity.class),
                is("SELECT "
                        + joinAllColumnNames(UsersEntity.class)
                        + " FROM USER_INFO "
                        + "WHERE "
                        + "ID=?"));
    }

    /**
     * 複数のIDを条件とするSELECT文が構築できること。
     *
     * @throws Exception
     */
    @Test
    public void testBuildSelectByIdFromMultiId() throws Exception {
        assertThat("複数のIDのEntity",
                sut.buildSelectByIdSql(MultiIdEntity.class),
                is("SELECT "
                        + joinAllColumnNames(MultiIdEntity.class)
                        + " FROM MULTI_ID_ENTITY "
                        + "WHERE "
                        + "ID=? "
                        + "AND NO=?"));
    }

    /**
     * {@link Table#schema()}指定されたEntityの場合、スキーマ修飾子が指定されたSQL文が生成されること
     */
    @Test
    public void testBuildSelectByIdWithSchema() throws Exception {
        assertThat("スキーマ修飾あり",
                sut.buildSelectByIdSql(WithSchemaEntity.class),
                is("SELECT "
                        + joinAllColumnNames(WithSchemaEntity.class)
                        + " FROM test_schema.TEST_ENTITY "
                        + "WHERE "
                        + "ID=?"));
    }

    /**
     * 全レコード検索のSELECT文が構築できること。
     */
    @Test
    public void testBuildSelectAll() throws Exception {
        assertThat(sut.buildSelectAllSql(UsersEntity.class),
                is("SELECT "
                        + joinAllColumnNames(UsersEntity.class)
                        + " FROM USER_INFO"));
    }

    /**
     * {@link Table#schema()}指定有りの場合、スキーマ修飾子が指定されたSQL文が生成されること
     */
    @Test
    public void testBuildSelectAllWithSchema() throws Exception {
        assertThat(sut.buildSelectAllSql(WithSchemaEntity.class),
                is("SELECT "
                        + joinAllColumnNames(WithSchemaEntity.class)
                        + " FROM test_schema.TEST_ENTITY"));
    }

    /**
     * バージョンカラムなしの更新用SQL文が生成できること
     */
    @Test
    public void testBuildUpdateSqlWithoutVersion() throws Exception {
        MultiIdEntity entity = new MultiIdEntity();
        SqlWithParams actual = sut.buildUpdateSql(entity);
        assertThat("UPDATE文が構築出来ていること", actual.getSql(),
                is("UPDATE MULTI_ID_ENTITY "
                        + "SET "
                        + makeSetClause(MultiIdEntity.class)
                        + " WHERE "
                        + "ID=? "
                        + "AND NO=?"));

        List<Object> params = actual.getParams();
        assertThat("バインドするパラメータが取得できること(条件は、setの後に続く)",
                params.size(),
                is(3));

        assertThat((String) params.get(0), is(""));
        assertThat((Long) params.get(1), is(12345L));
        assertThat(((String) params.get(2)), is(""));
    }

    /**
     * バージョンカラム有りの更新用SQLが生成できること
     */
    @Test
    public void testBuildUpdateSqlWithVersion() throws Exception {
        UsersEntity entity = new UsersEntity();
        SqlWithParams actual = sut.buildUpdateSql(entity);

        assertThat("UPDATE文が構築できていること", actual.getSql(),
                is("UPDATE USER_INFO "
                        + "SET "
                        + makeSetClause(UsersEntity.class)
                        + " WHERE "
                        + "ID=? "
                        + "AND VERSION=?"));

        Map<ColumnMeta, Object> columns = EntityUtil.findAllColumns(entity);
        int index = 0;
        List<Object> params = actual.getParams();
        for (Map.Entry<ColumnMeta, Object> entry : columns.entrySet()) {
            ColumnMeta column = entry.getKey();
            if (column.isIdColumn() || column.isVersion()) {
                continue;
            }
            assertThat(column.getName(),
                    params.get(index++),
                    is(entry.getValue()));
        }
        assertThat((Long) params.get(index++), is(0L));
        assertThat((Long) params.get(index++), is(999L));
    }

    /**
     * {@link Table#schema()}指定有りの場合、スキーマ修飾子が指定されたSQL文が生成されること
     */
    @Test
    public void testBuildUpdateSqlWithSchema() throws Exception {
        WithSchemaEntity entity = new WithSchemaEntity();
        SqlWithParams actual = sut.buildUpdateSql(entity);

        assertThat("UPDATE文が構築できていること", actual.getSql(),
                is("UPDATE test_schema.TEST_ENTITY "
                        + "SET "
                        + makeSetClause(WithSchemaEntity.class)
                        + " WHERE "
                        + "ID=?"));

        Map<ColumnMeta, Object> columns = EntityUtil.findAllColumns(entity);
        int index = 0;
        List<Object> params = actual.getParams();
        for (Map.Entry<ColumnMeta, Object> entry : columns.entrySet()) {
            ColumnMeta column = entry.getKey();
            if (column.isIdColumn() || column.isVersion()) {
                continue;
            }
            assertThat(column.getName(),
                    params.get(index++),
                    is(entry.getValue()));
        }
        assertThat((Long) params.get(index++), is(1L));
    }

    /**
     * Entityクラス以外からは更新用SQLが構築できないこと。
     *
     * @throws Exception
     */
    @Test(expected = IllegalEntityException.class)
    public void testBuildUpdateSqlFromNotEntity() throws Exception {
        sut.buildUpdateSql(new Object());
    }

    /**
     * バージョンカラムなしの一括更新(batch update)用のSQL文が構築できること
     */
    @Test
    public void buildBatchUpdateSqlWithoutVersion() throws Exception {

        BatchSqlWithColumns actual = sut.buildBatchUpdateSql(MultiIdEntity.class);
        assertThat("UPDATE文が構築出来ていること", actual.getSql(),
                is("UPDATE MULTI_ID_ENTITY "
                        + "SET "
                        + makeSetClause(MultiIdEntity.class)
                        + " WHERE "
                        + "ID=? "
                        + "AND NO=?"));

        List<ColumnMeta> columns = actual.getColumns();
        final List<String> columnNames = toColumnNames(columns);
        assertThat(columns.size(), is(3));

        assertThat(columnNames, contains("user_name", "ID", "NO"));
    }

    /**
     * バージョンカラム有りの一括更新用(batch update)SQLが生成できること
     */
    @Test
    public void testBuildBatchUpdateSqlWithVersion() throws Exception {
        BatchSqlWithColumns actual = sut.buildBatchUpdateSql(UsersEntity.class);

        assertThat("UPDATE文が構築できていること", actual.getSql(),
                is("UPDATE USER_INFO "
                        + "SET "
                        + makeSetClause(UsersEntity.class)
                        + " WHERE "
                        + "ID=? "
                        + "AND VERSION=?"));

        final List<ColumnMeta> columns = actual.getColumns();
        final List<String> columnNames = new ArrayList<String>();
        for (ColumnMeta column : columns) {
            columnNames.add(column.getName());
        }
        assertThat(columns.size(), is(4));

        assertThat("set句に主キーとバージョンカラム以外が含まれていること",
                columnNames.subList(0, 2), containsInAnyOrder("NAME", "BIRTHDAY"));
        assertThat("条件は主キー→バージョンカラムの順", columnNames.subList(2, 4), contains("ID", "VERSION"));
    }

    /**
     * {@link Table#schema()}指定有りの場合、スキーマ修飾子が指定されたSQL文が生成されること
     */
    @Test
    public void testBuildBatchUpdateSqlWithSchema() throws Exception {
        BatchSqlWithColumns actual = sut.buildBatchUpdateSql(WithSchemaEntity.class);

        assertThat("UPDATE文が構築できていること", actual.getSql(),
                is("UPDATE test_schema.TEST_ENTITY "
                        + "SET "
                        + makeSetClause(WithSchemaEntity.class)
                        + " WHERE "
                        + "ID=?"));

        final List<ColumnMeta> columns = actual.getColumns();
        List<String> columnNames = toColumnNames(columns);
        assertThat(columnNames, contains("NAME", "ID"));
    }

    /**
     * Entityクラス以外からは一括更新用SQLが構築できないこと。
     *
     */
    @Test(expected = IllegalEntityException.class)
    public void testBuildBatchUpdateSqlFromNotEntity() throws Exception {
        sut.buildBatchUpdateSql(Object.class);
    }

    /**
     * 削除用SQLが生成できること。
     *
     * @throws Exception
     */
    @Test
    public void testBuildDeleteSql() throws Exception {
        UsersEntity entity = new UsersEntity();
        SqlWithParams actual = sut.buildDeleteSql(entity);
        String sql = actual.getSql();

        assertThat(sql, is("DELETE FROM USER_INFO "
                + "WHERE "
                + "ID=?"));

        List<Object> params = actual.getParams();
        assertThat(params.size(), is(1));
        assertThat((Long) params.get(0), is(0L));
    }

    /**
     * {@link Table#schema()}を指定した場合、スキーマ修飾子が指定されたSQL文が生成されること。
     */
    @Test
    public void testBuildDeleteSqlWithSchema() throws Exception {
        final WithSchemaEntity entity = new WithSchemaEntity();
        final SqlWithParams actual = sut.buildDeleteSql(entity);

        assertThat(actual.getSql(), is("DELETE FROM test_schema.TEST_ENTITY "
                + "WHERE "
                + "ID=?"));

        List<Object> params = actual.getParams();
        assertThat(params.size(), is(1));
        assertThat((Long) params.get(0), is(1L));
    }

    /**
     * 一括削除用SQLが生成できること。
     */
    @Test
    public void testBuildBatchDeleteSql() throws Exception {
        BatchSqlWithColumns actual = sut.buildBatchDeleteSql(UsersEntity.class);
        String sql = actual.getSql();

        assertThat(sql, is("DELETE FROM USER_INFO "
                + "WHERE "
                + "ID=?"));

        final List<ColumnMeta> columns = actual.getColumns();
        assertThat(toColumnNames(columns), contains("ID"));
    }

    /**
     * {@link Table#schema()}を指定した場合、スキーマ修飾子が指定されたSQL文が生成されること。
     */
    @Test
    public void testBuildBatchDeleteSqlWithSchema() throws Exception {
        final WithSchemaEntity entity = new WithSchemaEntity();
        final BatchSqlWithColumns actual = sut.buildBatchDeleteSql(WithSchemaEntity.class);

        assertThat(actual.getSql(), is("DELETE FROM test_schema.TEST_ENTITY "
                + "WHERE "
                + "ID=?"));

        assertThat(toColumnNames(actual.getColumns()), contains("ID"));
    }

    /**
     * 登録用SQLが生成できること。
     *
     * @throws Exception
     */
    @Test
    public void testBuildInsertSql() throws Exception {
        UsersEntity entity = new UsersEntity();
        SqlWithParams actual = sut.buildInsertSql(entity);

        String sql = actual.getSql();
        List<Object> params = actual.getParams();

        assertThat(sql, is("INSERT INTO USER_INFO("
                + joinAllColumnNames(UsersEntity.class)
                + ")VALUES(?,?,?,?)"));

        assertThat("バインドパラメータ数", params.size(), is(4));
        Map<ColumnMeta, Object> columns = EntityUtil.findAllColumns(entity);

        int index = 0;
        for (Map.Entry<ColumnMeta, Object> entry : columns.entrySet()) {
            assertThat(entry.getKey()
                    .getName(), params.get(index++), is(entry.getValue()));
        }
    }

    /**
     * {@link Table#schema()}を指定した場合、スキーマ修飾子が指定されたSQL文が生成されること。
     */
    @Test
    public void testBuildInsertSqlWithSchema() throws Exception {
        WithSchemaEntity entity = new WithSchemaEntity();
        SqlWithParams actual = sut.buildInsertSql(entity);

        String sql = actual.getSql();
        List<Object> params = actual.getParams();

        assertThat(sql, is("INSERT INTO test_schema.TEST_ENTITY("
                + joinAllColumnNames(WithSchemaEntity.class)
                + ")VALUES(?,?)"));

        assertThat("バインドパラメータ数", params.size(), is(2));
        Map<ColumnMeta, Object> columns = EntityUtil.findAllColumns(entity);

        int index = 0;
        for (Map.Entry<ColumnMeta, Object> entry : columns.entrySet()) {
            assertThat(entry.getKey()
                    .getName(), params.get(index++), is(entry.getValue()));
        }
    }

    /**
     * Identityカラム以外を登録対象としたINSERT文が生成できること。
     *
     * @throws Exception
     */
    @Test
    public void testBuildInsertSqlWithIdentityColumn() throws Exception {
        UsersEntity entity = new UsersEntity();
        SqlWithParams actual = sut.buildInsertWithIdentityColumnSql(entity);

        String sql = actual.getSql();
        List<Object> params = actual.getParams();

        assertThat(sql, is("INSERT INTO USER_INFO("
                + joinAllColumnNamesWithOutIdentityColumn(UsersEntity.class)
                + ")VALUES(?,?,?)"));

        assertThat("バインドパラメータ数", params.size(), is(3));
        Map<ColumnMeta, Object> columns = EntityUtil.findAllColumns(entity);

        int index = 0;
        for (Map.Entry<ColumnMeta, Object> entry : columns.entrySet()) {
            ColumnMeta column = entry.getKey();
            if (column.isGeneratedValue()) {
                continue;
            }
            assertThat(column
                    .getName(), params.get(index++), is(entry.getValue()));
        }
    }

    /**
     * 一括登録用(batch insert用)SQL文が生成されること。
     */
    @Test
    public void testBuildBatchInsertSql() throws Exception {
        BatchSqlWithColumns actual = sut.buildBatchInsertSql(UsersEntity.class);

        String sql = actual.getSql();

        assertThat(sql, is("INSERT INTO USER_INFO("
                + joinAllColumnNames(UsersEntity.class)
                + ")VALUES(?,?,?,?)"));

        List<ColumnMeta> values = actual.getColumns();
        final List<ColumnMeta> expectedColumns = EntityUtil.findAllColumns(UsersEntity.class);
        assertThat("要素数が一致していること", values.size(), is(expectedColumns.size()));

        int index = 0;
        for (ColumnMeta column : expectedColumns) {
            assertThat(column.getName(), values.get(index), is(column));
            index++;
        }
    }

    /**
     * 一括登録用(batch insert用)SQL文が生成されること。
     */
    @Test
    public void testBuildBatchInsertSqlWithSchema() throws Exception {
        BatchSqlWithColumns actual = sut.buildBatchInsertSql(WithSchemaEntity.class);

        String sql = actual.getSql();

        assertThat(sql, is("INSERT INTO test_schema.TEST_ENTITY("
                + joinAllColumnNames(WithSchemaEntity.class)
                + ")VALUES(?,?)"));

        List<ColumnMeta> values = actual.getColumns();
        final List<ColumnMeta> expectedColumns = EntityUtil.findAllColumns(WithSchemaEntity.class);
        assertThat("要素数が一致していること", values.size(), is(expectedColumns.size()));

        int index = 0;
        for (ColumnMeta column : expectedColumns) {
            assertThat(column.getName(), values.get(index), is(column));
            index++;
        }
    }

    /**
     * 一括登録用(batch insert用)の自動採番カラムが含まれないSQL文が生成されること
     */
    @Test
    public void testBuildBatchInsertWithIdentityColumnSql() throws Exception {
        BatchSqlWithColumns actual = sut.buildBatchInsertWithIdentityColumnSql(UsersEntity.class);

        String sql = actual.getSql();

        assertThat(sql, is("INSERT INTO USER_INFO("
                + joinAllColumnNamesWithOutIdentityColumn(UsersEntity.class)
                + ")VALUES(?,?,?)"));

        List<ColumnMeta> values = actual.getColumns();
        final List<ColumnMeta> expectedColumns = EntityUtil.findAllColumns(UsersEntity.class);
        assertThat("自動採番カラムの1カラム少ないこと", values.size(), is(expectedColumns.size() - 1));

        int index = 0;
        for (ColumnMeta column : expectedColumns) {
            if (column.isGeneratedValue()) {
                continue;
            }
            assertThat(column.getName(), values.get(index), is(column));
            index++;
        }
    }

    // ---------------------------------------- test entity
    @Table(name = "USER_INFO")
    @Entity
    public static class UsersEntity {

        @Id
        @GeneratedValue(strategy = GenerationType.AUTO)
        public Long getId() {
            return 0L;
        }

        public String getName() {
            return "";
        }

        public Date getBirthday() {
            return null;
        }

        @Version
        public Long getVersion() {
            return 999L;
        }
    }

    @Entity
    public static class MultiIdEntity {

        @Id
        public Long getId() {
            return 12345L;
        }

        @Id
        public String getNo() {
            return "";
        }

        @Column(name = "user_name")
        public String getName() {
            return "";
        }
    }

    @Entity
    @Table(name = "TEST_ENTITY", schema = "test_schema")
    public static class WithSchemaEntity {

        @Id
        public Long getId() {
            return 1L;
        }

        public String getName() {
            return "name";
        }
    }

    // ---------------------------------------- helper method

    /**
     * Entityの全てのカラム名を","で連結する。
     *
     * @param entity エンティティ
     * @param <T> 型
     * @return カラム名を","で連結したもの
     */
    private static <T> String joinAllColumnNames(Class<T> entity) {
        List<String> columnNames = toColumnNames(EntityUtil.findAllColumns(entity));
        return StringUtil.join(",", columnNames);
    }

    /**
     * Entityの採番対象絡む以外を","を連結する。
     *
     * @param entity エンティティ
     * @param <T> 型
     * @return カラム名を","で連結したもの(採番対象カラムを除く)
     */
    private static <T> String joinAllColumnNamesWithOutIdentityColumn(Class<T> entity) {
        List<ColumnMeta> columns = EntityUtil.findAllColumns(entity);
        List<String> columnNames = new ArrayList<String>();
        for (ColumnMeta column : columns) {
            if (column.isGeneratedValue()) {
                continue;
            }
            columnNames.add(column.getName());
        }
        return StringUtil.join(",", columnNames);
    }


    /**
     * 更新用SQL文のSET句を構築する。
     *
     * @param entity エンティティ
     * @param <T> 型
     * @return 構築したSET句
     */
    private static <T> String makeSetClause(Class<T> entity) {
        List<ColumnMeta> columns = EntityUtil.findAllColumns(entity);
        List<String> columnNames = new ArrayList<String>();
        for (ColumnMeta column : columns) {
            if (column.isIdColumn()) {
                continue;
            }
            if (column.isVersion()) {
                String versionColumnName = column.getName();
                columnNames.add(versionColumnName + '=' + versionColumnName + "+1");
            } else {
                columnNames.add(column.getName() + "=?");
            }
        }
        return StringUtil.join(",", columnNames);
    }

    private static List<String> toColumnNames(List<ColumnMeta> columns) {
        List<String> columnNames = new ArrayList<String>();
        for (ColumnMeta column : columns) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

}

