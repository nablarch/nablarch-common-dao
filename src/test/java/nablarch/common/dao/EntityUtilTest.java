package nablarch.common.dao;

import static nablarch.common.dao.EntityUtil.getTableName;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.TableGenerator;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import nablarch.common.dao.DaoTestHelper.Address;
import nablarch.common.dao.DaoTestHelper.Users;
import nablarch.core.beans.BeanUtil;
import nablarch.core.beans.BeansException;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.transaction.JdbcTransactionFactory;
import nablarch.core.transaction.Transaction;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;


/**
 * {@link EntityUtil}に関するテストクラス
 */
@RunWith(Enclosed.class)
public class EntityUtilTest {

    /**
     * {@link EntityUtil#getTableName(Class)}のテスト
     */
    public static class GetTableNameClass {

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        @After
        public void tearDown() {
            DbConnectionContext.removeConnection();
        }

        /**
         * Tableアノテーションが設定されていないクラスの場合のケース
         */
        @Test
        public void noAnnotationClass() {
            class HogeTable {

            }
            assertThat("クラス名がテーブル名となる", getTableName(HogeTable.class), is("HOGE_TABLE"));
        }

        /**
         * Tableアノテーションのname属性が設定されていないクラスの場合のケース
         */
        @Test
        public void noNameAttributeClass() {
            @Table
            class FugaTable {

            }
            assertThat("クラス名がテーブル名となる", getTableName(FugaTable.class), is("FUGA_TABLE"));
        }

        @Test
        public void nameAttributeClass() {
            @Table(name = "HOGE_HOGE_TABLE")
            class Hoge {

            }
            assertThat("クラス名がテーブル名となる", getTableName(Hoge.class), is("HOGE_HOGE_TABLE"));
        }
    }

    /**
     * {@link EntityUtil#getSchemaName(Class)}のテスト。
     */
    public static class GetSchemaName {

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        @After
        public void tearDown() {
            DbConnectionContext.removeConnection();
        }

        /**
         * {@link Table}アノテーションが設定されていない場合は、スキーマ名はnull
         */
        @Test
        public void noAnnotationClass() {
            class HogeTable {}

            assertThat("スキーマ名はnull", EntityUtil.getSchemaName(HogeTable.class), is(nullValue()));
        }

        /**
         * {@link Table}アノテーションが設定されているが、schema属性が未設定の場合はnull
         */
        @Test
        public void withoutSchemaAttribute() {
            @Table(name = "hoge")
            class HogeTable {}

            assertThat("スキーマ名はnull", EntityUtil.getSchemaName(HogeTable.class), is(nullValue()));
        }

        /**
         * {@link Table}アノテーションが設定されていてschema属性が空文字列の場合、スキーマ名はnull
         */
        @Test
        public void schemaAttributeEmptyString() {
            @Table(name = "hoge", schema = "")
            class HogeTable {
            }
            assertThat("スキーマ名はnull", EntityUtil.getSchemaName(HogeTable.class), is(nullValue()));
        }

        /**
         * {@link Table}アノテーションが設定されていてschema属性に値がある場合、その値がスキーマ名となること
         */
        @Test
        public void withSchemaAttribute() {
            @Table(name = "hoge", schema = "test_schema")
            class HogeTable {
            }
            assertThat("スキーマ名はnull", EntityUtil.getSchemaName(HogeTable.class), is("test_schema"));

        }
    }

    /**
     * {@link EntityUtil#getTableNameWithSchema(Class)}のテスト。
     */
    public static class GetTableNameWithSchema {

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        @After
        public void tearDown() {
            DbConnectionContext.removeConnection();
        }

        /**
         * {@link Table}アノテーションが設定されていない場合は、テーブル名のみが取得される。
         */
        @Test
        public void noAnnotationClass() {
            class HogeTable {}

            assertThat("テーブル名のみが取得できる", EntityUtil.getTableNameWithSchema(HogeTable.class), is("HOGE_TABLE"));
        }

        /**
         * {@link Table#schema()}が設定されていない場合、テーブル名のみが取得される。
         */
        @Test
        public void withoutSchemaAttribute() {
            @Table(name = "hoge_entity")
            class HogeTable {}

            assertThat("テーブル名のみが取得できる", EntityUtil.getTableNameWithSchema(HogeTable.class), is("hoge_entity"));
        }

        /**
         * {@link Table#schema()}が空文字列の場合、テーブル名のみが取得される。
         */
        @Test
        public void schemaAttributeEmptyString() {
            @Table(name = "hoge_table", schema = "")
            class HogeTable {}

            assertThat("テーブル名のみが取得できる", EntityUtil.getTableNameWithSchema(HogeTable.class), is("hoge_table"));
        }

        /**
         * {@link Table#schema()}が設定されている場合、スキーマ名を修飾したテーブル名が取得される。
         */
        @Test
        public void withSchemaAttribute() {
            @Table(name = "hoge_table", schema = "schema_name")
            class HogeTable {}

            assertThat("スキーマ名付きのテーブル名が取得できる", EntityUtil.getTableNameWithSchema(HogeTable.class), is("schema_name.hoge_table"));
        }
    }


    /**
     * {@link EntityUtil#findIdColumns(Class)}のテスト。
     */
    @RunWith(DatabaseTestRunner.class)
    public static class FindIdColumnsFromClass {

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        private static AppDbConnection getConnection() {
            ConnectionFactory connectionFactory = repositoryResource.getComponent("connectionFactory");
            return connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        }

        private static Transaction getTransaction() {
            JdbcTransactionFactory jdbcTransactionFactory = repositoryResource.getComponent("jdbcTransactionFactory");
            return jdbcTransactionFactory.getTransaction(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        }

        @Before
        public void setUp() {
            DbConnectionContext.setConnection(getConnection());
            TransactionContext.setTransaction(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY, getTransaction());
        }

        @After
        public void tearDown() {
            Transaction transaction = TransactionContext.getTransaction();
            try {
                transaction.rollback();
            } catch (Exception ignore) {
            }
            final TransactionManagerConnection connection = (TransactionManagerConnection) DbConnectionContext.getConnection();
            try {
                connection.terminate();
            } catch (Exception ignore) {
            }
            TransactionContext.removeTransaction();
            DbConnectionContext.removeConnection();
        }

        /**
         * IDカラムが存在しないクラスの場合
         */
        @Test
        public void notId() {
            class Entity {

            }
            List<ColumnMeta> idColumns = EntityUtil.findIdColumns(Entity.class);
            assertThat("IDカラムは存在しない", idColumns.isEmpty(), is(true));
        }

        /**
         * IDカラムが１つだけ存在するクラスの場合
         */
        @Test
        public void singleId() {
            VariousDbTestHelper.createTable(Users.class);
            List<ColumnMeta> idColumns = EntityUtil.findIdColumns(Users.class);
            assertThat("IDカラムは1つだけ", idColumns.size(), is(1));
            assertThat("IDカラムは、'USER_ID'", idColumns.get(0)
                    .getName(), is("USER_ID"));
            assertThat(idColumns.get(0)
                    .isIdColumn(), is(true));
        }

        /**
         * IDカラムが複数存在するクラス。
         */
        @Test
        public void multiId() {
            VariousDbTestHelper.createTable(Address.class);
            List<ColumnMeta> idColumns = EntityUtil.findIdColumns(Address.class);
            assertThat("IDカラムは2つ", idColumns.size(), is(2));
            assertThat("1つめのIDはADDRESS_ID", idColumns.get(0)
                    .getName(), is("ADDRESS_ID"));
            assertThat(idColumns.get(0)
                    .isIdColumn(), is(true));
            assertThat("2つめのIDはADDRESS_CODE", idColumns.get(1)
                    .getName(), is("ADDRESS_CODE"));
            assertThat(idColumns.get(1)
                    .isIdColumn(), is(true));
        }
    }

    /**
     * {@link EntityUtil#findIdColumns(Object)}のテスト
     */
    public static class FindIdColumnsFromEntity {

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        @After
        public void tearDown() {
            DbConnectionContext.removeConnection();
        }

        /**
         * IDカラムが存在しないクラスのケース
         */
        @Test
        public void notId() {
            class Entity {

            }
            Map<ColumnMeta, Object> idColumns = EntityUtil.findIdColumns(new Entity());
            assertThat("IDカラムは存在しないので空", idColumns.isEmpty(), is(true));
        }

        public static class SingleId {

            @Id
            public Integer getId() {
                return 1;
            }

            public String getName() {
                return "hoge";
            }
        }

        /**
         * IDカラムが１つだけのクラスのケース
         */
        @Test
        public void singleId() {
            Map<ColumnMeta, Object> ids = EntityUtil.findIdColumns(new SingleId());
            assertThat("サイズは1", ids.size(), is(1));
            List<ColumnMeta> idColumns = EntityUtil.findIdColumns(SingleId.class);
            for (ColumnMeta column : idColumns) {
                assertThat("値", (Integer) ids.get(column), is(1));
            }
        }

        public static class MultiId {

            @Id
            public Integer getId() {
                return 100;
            }

            @Id
            public String getYmd() {
                return "20140102";
            }

            public String getName() {
                return "name";
            }
        }

        /**
         * IDカラムが複数のクラスのケース
         */
        @Test
        public void multiId() {
            Map<ColumnMeta, Object> ids = EntityUtil.findIdColumns(new MultiId());
            List<ColumnMeta> idColumns = EntityUtil.findIdColumns(MultiId.class);
            assertThat("IDのカラムは2つ", ids.size(), is(2));
            for (ColumnMeta column : idColumns) {
                assertThat("値が取得できること", ids.get(column), is(notNullValue()));
            }
        }
    }

    /**
     * {@link EntityUtil#findAllColumns(Class)}及び{@link EntityUtil#findAllColumns(Object)}のテストクラス。
     */
    public static class FindAllColumnsFromClass {

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        @After
        public void tearDown() {
            DbConnectionContext.removeConnection();
        }

        private static ColumnMeta findColumn(List<ColumnMeta> columns, String columnName) {
            for (ColumnMeta column : columns) {
                if (column.getName()
                        .equals(columnName)) {
                    return column;
                }
            }
            throw new IllegalArgumentException("column not found. column name = " + columnName);
        }

        public static class Hoge {

            private final long time = System.currentTimeMillis();

            @Id
            @GeneratedValue(strategy = GenerationType.TABLE, generator = "SEQ")
            @TableGenerator(name = "SEQ", pkColumnValue = "GEN_ID")
            @Column
            public Long getId() {
                return 1L;
            }

            public String getUserName() {
                return "userName";
            }

            public BigDecimal getAmount() {
                return new BigDecimal("1.0");
            }

            public java.sql.Date getSqlDate() {
                return new java.sql.Date(time);
            }

            public Timestamp getTimestamp() {
                return new Timestamp(time);
            }

            public Time getTime() {
                return new Time(time);
            }

            @Temporal(TemporalType.DATE)
            public Date getTemporalDate() {
                return new Date(time);
            }

            @Temporal(TemporalType.TIMESTAMP)
            public Calendar getTemporalTimestamp() {
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(time);
                return calendar;
            }

            @Temporal(TemporalType.TIME)
            public Date getTemporalTime() {
                return new Date(time);
            }

            @Column(name = "SHORT_COL")
            public Short getShort() {
                return 99;
            }

            @Version
            public Integer getVersion() {
                return 1;
            }

            // 対象外カラム
            @Transient
            public String getHoge() {
                return "hoge";
            }

            @JoinColumn
            public String getJoinColumn() {
                return "";
            }

            @OneToMany
            public String getOneToMany() {
                return "";
            }

            @ManyToOne
            public String getManyToOne() {
                return "";
            }

            @ManyToMany
            public String getmanyToMany() {
                return "";
            }

            @OneToOne
            public String getOneToOne() {
                return "";
            }
        }


        /**
         * 全ての属性情報が取得できること。
         */
        @Test
        public void findAllColumnsFromEntityClass() {
            List<ColumnMeta> columns = EntityUtil.findAllColumns(Hoge.class);

            IDカラム:
            {
                ColumnMeta column = findColumn(columns, "ID");
                assertThat("主キーも含まれる", column.getName(), is("ID"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("id"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(Long.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(Long.class));
                assertThat("採番タイプが取得できる", column.getGenerationType(), is(GenerationType.TABLE));
                assertThat("採番の名称が取得できる", column.getGeneratorName(), is("GEN_ID"));
                assertThat("採番対象カラム", column.isGeneratedValue(), is(true));
                assertThat("IDカラム", column.isIdColumn(), is(true));
                assertThat("バージョンカラムではない", column.isVersion(), is(false));
            }

            userNameカラム:
            {
                ColumnMeta column = findColumn(columns, "USER_NAME");
                assertThat("カラム名が取得できる", column.getName(), is("USER_NAME"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("userName"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(String.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(String.class));
                assertThat("採番タイプはnull", column.getGenerationType(), is(nullValue()));
                assertThat("採番の名称もnull", column.getGeneratorName(), is(nullValue()));
                assertThat("採番対象カラムではない", column.isGeneratedValue(), is(false));
                assertThat("IDカラムではない", column.isIdColumn(), is(false));
                assertThat("バージョンカラムではない", column.isVersion(), is(false));
            }

            バージョンカラム:
            {
                ColumnMeta column = findColumn(columns, "VERSION");
                assertThat("カラム名が取得できる", column.getName(), is("VERSION"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("version"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(Integer.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(Integer.class));
                assertThat("採番タイプはnull", column.getGenerationType(), is(nullValue()));
                assertThat("採番の名称もnull", column.getGeneratorName(), is(nullValue()));
                assertThat("採番対象カラムではない", column.isGeneratedValue(), is(false));
                assertThat("IDカラムではない", column.isIdColumn(), is(false));
                assertThat("バージョンカラム", column.isVersion(), is(true));
            }

            BigDecimalカラム:
            {
                ColumnMeta column = findColumn(columns, "AMOUNT");
                assertThat("カラム名が取得できる", column.getName(), is("AMOUNT"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("amount"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(BigDecimal.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(BigDecimal.class));
            }

            sql_dateカラム:
            {
                ColumnMeta column = findColumn(columns, "SQL_DATE");
                assertThat("カラム名が取得できる", column.getName(), is("SQL_DATE"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("sqlDate"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(
                        java.sql.Date.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(java.sql.Date.class));
            }

            sql_dateカラム:
            {
                ColumnMeta column = findColumn(columns, "TIMESTAMP");
                assertThat("カラム名が取得できる", column.getName(), is("TIMESTAMP"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("timestamp"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(Timestamp.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(Timestamp.class));
            }

            sql_timestampカラム:
            {
                ColumnMeta column = findColumn(columns, "TIMESTAMP");
                assertThat("カラム名が取得できる", column.getName(), is("TIMESTAMP"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("timestamp"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(Timestamp.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(Timestamp.class));
            }

            sql_timeカラム:
            {
                ColumnMeta column = findColumn(columns, "TIME");
                assertThat("カラム名が取得できる", column.getName(), is("TIME"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("time"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(Time.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(Time.class));
            }

            temporal_dateカラム:
            {
                ColumnMeta column = findColumn(columns, "TEMPORAL_DATE");
                assertThat("カラム名が取得できる", column.getName(), is("TEMPORAL_DATE"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("temporalDate"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(Date.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(java.sql.Date.class));
            }

            temporal_timestampカラム:
            {
                ColumnMeta column = findColumn(columns, "TEMPORAL_TIMESTAMP");
                assertThat("カラム名が取得できる", column.getName(), is("TEMPORAL_TIMESTAMP"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("temporalTimestamp"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(Calendar.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(Timestamp.class));
            }

            temporal_timeカラム:
            {
                ColumnMeta column = findColumn(columns, "TEMPORAL_TIME");
                assertThat("カラム名が取得できる", column.getName(), is("TEMPORAL_TIME"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("temporalTime"));
                assertThat("プロパティの型が取得できる", column.getPropertyType(), CoreMatchers.equalTo(Date.class));
                assertThat("JDBCの型が取得できる", column.getJdbcType(), CoreMatchers.equalTo(Time.class));
            }

            Columnアノテーションでカラム名を指定した場合:
            {
                ColumnMeta column = findColumn(columns, "SHORT_COL");
                assertThat("カラム名が取得できる", column.getName(), is("SHORT_COL"));
                assertThat("プロパティ名が取得できる", column.getPropertyName(), is("short"));
            }
        }

        /**
         * {@link EntityUtil#findAllColumns(Class)}のテスト。
         */
        @Test
        public void findAllColumnsFromEntityInstance() {

            Hoge hoge = new Hoge();
            Map<ColumnMeta, Object> columns = EntityUtil.findAllColumns(hoge);

            List<ColumnMeta> columnMetas = EntityUtil.findAllColumns(Hoge.class);
            for (ColumnMeta meta : columnMetas) {
                Object actual = columns.get(meta);
                assertThat(meta.getName(), actual,
                        is(BeanUtil.getProperty(hoge, meta.getPropertyName(), meta.getJdbcType())));
            }
        }

        /**
         * エンティティでないBeanクラス
         */
        public static class FugaDto {
            private BigDecimal bigDecimal;
            private String string;

            public BigDecimal getBigDecimal() {
                return bigDecimal;
            }

            public void setBigDecimal(BigDecimal bigDecimal) {
                this.bigDecimal = bigDecimal;
            }

            public String getString() {
                return string;
            }

            public void setString(String string) {
                this.string = string;
            }
        }
        @Test
        public void findAllColumnsFromNotEntityClass() {
            List<ColumnMeta> actual = EntityUtil.findAllColumns(FugaDto.class);

            ColumnMeta bigDecimal = findColumn(actual, "BIG_DECIMAL");
            assertThat(bigDecimal.getName(), is("BIG_DECIMAL"));
            assertThat(bigDecimal.getPropertyName(), is("bigDecimal"));
            assertThat(bigDecimal.getPropertyType(), CoreMatchers.equalTo(BigDecimal.class));
            assertThat(bigDecimal.getJdbcType(), CoreMatchers.equalTo(BigDecimal.class));
            assertThat(bigDecimal.getGenerationType(), is(nullValue()));
            assertThat(bigDecimal.getGeneratorName(), is(nullValue()));

            ColumnMeta string = findColumn(actual, "STRING");
            assertThat(string.getName(), is("STRING"));
            assertThat(string.getPropertyName(), is("string"));
            assertThat(string.getPropertyType(), CoreMatchers.equalTo(String.class));
            assertThat(string.getJdbcType(), CoreMatchers.equalTo(String.class));
            assertThat(string.getGenerationType(), is(nullValue()));
            assertThat(string.getGeneratorName(), is(nullValue()));
        }

    }

    /**
     * {@link EntityUtil#findVersionColumn(Object)}のテストクラス。
     */
    public static class FindVersionColumn {

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        @After
        public void tearDown() {
            DbConnectionContext.removeConnection();
        }

        @Rule
        public ExpectedException exception = ExpectedException.none();

        /**
         * バージョンカラムが存在しないクラスの場合
         */
        @Test
        public void notVersionCol() {
            class Hoge {

            }
            ColumnMeta versionColumn = EntityUtil.findVersionColumn(new Hoge());
            assertThat("バージョンカラムが存在しないのでnullが戻る", versionColumn, is(nullValue()));
        }

        /**
         * バージョンカラムが存在するクラスの場合
         *
         */
        @Test
        public void existsVersionCol() {
            class Hoge {

                @Version
                public long getVersion() {
                    return 1;
                }
            }
            ColumnMeta versionColumn = EntityUtil.findVersionColumn(new Hoge());
            assertThat(versionColumn.getName(), is("VERSION"));
            assertThat(versionColumn.getPropertyName(), is("version"));
        }

        /**
         * バージョンカラムが複数存在した場合エラーとなること
         */
        @Test
        public void multiVersionCol() {
            class Hoge {

                @Version
                public long getVersion1() {
                    return 1;
                }

                @Version
                public long getVersion2() {
                    return 1;
                }
            }
            exception.expect(IllegalEntityException.class);
            EntityUtil.findVersionColumn(new Hoge());
        }
    }

    /**
     * {@link EntityUtil#findGeneratedValueColumn(Object)}のテストクラス。
     */
    public static class FindGeneratedValueColumn {

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        @After
        public void tearDown() {
            DbConnectionContext.removeConnection();
        }

        @Rule
        public ExpectedException exception = ExpectedException.none();

        /**
         * 採番対象カラムが存在しないクラスの場合
         */
        @Test
        public void notGeneratedValueCol() {
            class Hoge {

            }
            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("採番対象のカラムが存在しないのでnullが戻る", generatedValueColumn, is(nullValue()));
        }

        /**
         * 採番対象カラムが存在するクラスの場合
         */
        @Test
        public void existsGeneratedValueCol() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.AUTO)
                public Long getId() {
                    return 1L;
                }
            }
            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat(generatedValueColumn.getName(), is("ID"));
            assertThat(generatedValueColumn.getGenerationType(), is(GenerationType.AUTO));
            assertThat(generatedValueColumn.getGeneratorName(), is("HOGE_ID"));
        }

        @Test
        public void multiGeneratedValue() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.AUTO)
                public Long getId() {
                    return 1L;
                }

                @GeneratedValue(strategy = GenerationType.AUTO)
                public Long getId2() {
                    return 1L;
                }
            }
            exception.expect(IllegalEntityException.class);
            EntityUtil.findGeneratedValueColumn(new Hoge());
        }

        /**
         * 採番対象カラムのstrategyでシーケンスを指定している場合
         */
        @Test
        public void generatedValueSequence() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ")
                @SequenceGenerator(name = "SEQ", sequenceName = "SEQ1")
                public Long getSequenceGen() {
                    return 100L;
                }
            }

            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("SEQUENCE_GEN"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("sequenceGen"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.SEQUENCE));
            assertThat("シーケンスアノテーションに設定したシーケンス名称が採番名称", generatedValueColumn.getGeneratorName(), is("SEQ1"));
        }

        /**
         * 採番対象カラムのstrategyでシーケンスでシーケンス名を設定していない場合
         *
         */
        @Test
        public void generatedValueSequenceWithoutGenerator() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.SEQUENCE)
                public Long getSequenceGenNotGenName() {
                    return 101L;
                }
            }
            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("SEQUENCE_GEN_NOT_GEN_NAME"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("sequenceGenNotGenName"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.SEQUENCE));
            assertThat("テーブル名_カラム名が採番名称となる", generatedValueColumn.getGeneratorName(), is(
                    "HOGE_SEQUENCE_GEN_NOT_GEN_NAME"));
        }

        /**
         * 採番対象カラムのstrategyがシーケンスで、generatorに対応するSequenceValueアノテーションが存在しない場合
         */
        @Test
        public void generatedValueSequenceWithoutSeqGenAnnotation() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ")
                public Long getSequenceGenNotSeqAnnotation() {
                    return 102L;
                }
            }
            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("SEQUENCE_GEN_NOT_SEQ_ANNOTATION"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("sequenceGenNotSeqAnnotation"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.SEQUENCE));
            assertThat("テーブル名_カラム名が採番名称となる", generatedValueColumn.getGeneratorName(), is(
                    "HOGE_SEQUENCE_GEN_NOT_SEQ_ANNOTATION"));
        }

        /**
         * 採番対象カラムのstrategyがtableで、SequenceGeneratorアノテーションのname属性と一致しない場合
         */
        @Test
        public void generatedValueSequenceNotEqualsGenName() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ")
                @SequenceGenerator(name = "SEQ1")
                public Long getSeqGen() {
                    return 103L;
                }
            }

            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("SEQ_GEN"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("seqGen"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.SEQUENCE));
            assertThat("テーブル名_カラム名が採番名称となる", generatedValueColumn.getGeneratorName(), is("HOGE_SEQ_GEN"));
        }

        /**
         * 採番対象カラムのstrategyがシーケンスで、SequenceGeneratorアノテーションにシーケンス名が設定されていない場合
         */
        @Test
        public void generatedValueSequenceWithoutSequenceName() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "SEQ")
                @SequenceGenerator(name = "SEQ")
                public Long getSequenceGenNotEqualName() {
                    return 103L;
                }
            }

            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("SEQUENCE_GEN_NOT_EQUAL_NAME"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("sequenceGenNotEqualName"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.SEQUENCE));
            assertThat("テーブル名_カラム名が採番名称となる", generatedValueColumn.getGeneratorName(), is(
                    "HOGE_SEQUENCE_GEN_NOT_EQUAL_NAME"));
        }

        /**
         * 採番対象カラムのstrategyがautoで、SequenceGeneratorアノテーションが設定されている場合
         */
        @Test
        public void generatedValueAutoAndSequenceGenerator() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.AUTO, generator = "SEQ")
                @SequenceGenerator(name = "SEQ", sequenceName = "AUTO_SEQ")
                public Long getAutoGenSequence() {
                    return 104L;
                }
            }

            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());

            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("AUTO_GEN_SEQUENCE"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("autoGenSequence"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.SEQUENCE));
            assertThat("シーケンス名称が取得出来ること", generatedValueColumn.getGeneratorName(), is("AUTO_SEQ"));
        }

        /**
         * 採番対象カラムのstrategyがautoで、TableGeneratorアノテーションが設定されている場合
         */
        @Test
        public void generatedValueAutoAndTableGenerator() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.AUTO, generator = "SEQ")
                @TableGenerator(name = "SEQ", pkColumnValue = "TABLE_SEQ")
                public Long getAutoGenTable() {
                    return 104L;
                }
            }
            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("AUTO_GEN_TABLE"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("autoGenTable"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.TABLE));
            assertThat("テーブルの採番名称が取得できること", generatedValueColumn.getGeneratorName(), is("TABLE_SEQ"));
        }

        /**
         * 採番対象カラムのstrategyがautoで、Generatorの指定がない場合
         */
        @Test
        public void generatedValueAutoWithGenerator() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.AUTO, generator = "SEQ")
                public Long getAutoGenNotType() {
                    return 110L;
                }
            }
            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("AUTO_GEN_NOT_TYPE"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("autoGenNotType"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.AUTO));
            assertThat("テーブルの採番名称が取得できること", generatedValueColumn.getGeneratorName(), is("HOGE_AUTO_GEN_NOT_TYPE"));
        }

        /**
         * 採番対象カラムのstrategyがtableで、TableGeneratorの指定がある場合
         */
        @Test
        public void generatedValueTableGen() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.TABLE, generator = "SEQ")
                @TableGenerator(name = "SEQ", pkColumnValue = "01")
                public Long getTableGen() {
                    return 105L;
                }
            }
            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("TABLE_GEN"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("tableGen"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.TABLE));
            assertThat("テーブルの採番名称が取得できること", generatedValueColumn.getGeneratorName(), is("01"));
        }

        /**
         * 採番対象カラムのstrategyがtableでgeneratorの指定がない場合
         */
        @Test
        public void generatedValueTableGenWithoutGenerator() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.TABLE)
                public Long getTableGenNotGenName() {
                    return 106L;
                }
            }

            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("TABLE_GEN_NOT_GEN_NAME"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("tableGenNotGenName"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.TABLE));
            assertThat("テーブル名_カラム名が採番名称となる", generatedValueColumn.getGeneratorName(), is(
                    "HOGE_TABLE_GEN_NOT_GEN_NAME"));
        }

        /**
         * 採番対象カラムのstrategyがtableで、generatorに対応するTableGeneratorアノテーションが存在しない場合
         */
        @Test
        public void generatedValueTableWithoutTableGenAnnotation() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.TABLE, generator = "SEQ")
                public Long getTableGenNotTableAnnotation() {
                    return 107L;
                }
            }
            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("TABLE_GEN_NOT_TABLE_ANNOTATION"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("tableGenNotTableAnnotation"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.TABLE));
            assertThat("テーブル名_カラム名が採番名称となる", generatedValueColumn.getGeneratorName(), is(
                    "HOGE_TABLE_GEN_NOT_TABLE_ANNOTATION"));
        }

        /**
         * 採番対象カラムのstrategyがtableで、SequenceGeneratorアノテーションのname属性と一致しない場合
         */
        @Test
        public void generatedValueTableNotEqualsGenName() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.TABLE, generator = "SEQ")
                @TableGenerator(name = "hoge")
                public Long getTableGen() {
                    return 107L;
                }
            }
            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("TABLE_GEN"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("tableGen"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.TABLE));
            assertThat("テーブル名_カラム名が採番名称となる", generatedValueColumn.getGeneratorName(), is("HOGE_TABLE_GEN"));
        }

        /**
         * 採番対象カラムのstrategyがtableで、SequenceGeneratorアノテーションにシーケンス名が設定されていない場合
         */
        @Test
        public void generatedValueTableWithoutSequenceName() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.TABLE, generator = "SEQ")
                @TableGenerator(name = "SEQ")
                public Long getTableGenNotEqualName() {
                    return 108L;
                }
            }

            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("TABLE_GEN_NOT_EQUAL_NAME"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("tableGenNotEqualName"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.TABLE));
            assertThat("テーブル名_カラム名が採番名称となる", generatedValueColumn.getGeneratorName(), is(
                    "HOGE_TABLE_GEN_NOT_EQUAL_NAME"));
        }

        /**
         * 採番対象カラムのstrategyがidentityの場合
         */
        @Test
        public void generatedValueIdentity() {
            class Hoge {

                @GeneratedValue(strategy = GenerationType.IDENTITY)
                public Long getUnsupportedGenType() {
                    return 109L;
                }
            }

            ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(new Hoge());
            assertThat("カラム名が取得できる", generatedValueColumn.getName(), is("UNSUPPORTED_GEN_TYPE"));
            assertThat("プロパティ名が取得できる", generatedValueColumn.getPropertyName(), is("unsupportedGenType"));
            assertThat("採番対象のカラム", generatedValueColumn.isGeneratedValue(), is(true));
            assertThat("採番タイプはシーケンス", generatedValueColumn.getGenerationType(), is(GenerationType.IDENTITY));
            assertThat("テーブル名_カラム名が採番名称となる", generatedValueColumn.getGeneratorName(), is(nullValue()));
        }
    }

    /**
     * {@link EntityUtil#createEntity(Class, SqlRow)}のテストクラス。
     */
    public static class CreateEntity {

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        @After
        public void tearDown() {
            DbConnectionContext.removeConnection();
        }

        public static class Hoge {

            private String stringType;

            private Short shortType;

            private short primitiveShort;

            private Integer integerType;

            private int primitiveInt;

            private Long longType;

            private long primitiveLong;

            private BigDecimal bigDecimalType;

            private Boolean booleanType;

            private boolean primitiveBoolean;

            private Date dateType;

            private Date dateTimestampType;
            
            private Timestamp timestampType;

            private LocalDate localDateType;

            private LocalDateTime localDateTimeType;

            private byte[] byteArray;

            private List<String> listType;

            private int[] intArray;

            private char invalid;

            private String noSetter;

            public void setStringType(String stringType) {
                this.stringType = stringType;
            }

            public String getStringType() {
                return stringType;
            }

            public Short getShortType() {
                return shortType;
            }

            public void setShortType(Short shortType) {
                this.shortType = shortType;
            }

            public short getPrimitiveShort() {
                return primitiveShort;
            }

            public void setPrimitiveShort(short primitiveShort) {
                this.primitiveShort = primitiveShort;
            }

            public Integer getIntegerType() {
                return integerType;
            }

            public void setIntegerType(Integer integerType) {
                this.integerType = integerType;
            }

            public int getPrimitiveInt() {
                return primitiveInt;
            }

            public void setPrimitiveInt(int primitiveInt) {
                this.primitiveInt = primitiveInt;
            }

            public Long getLongType() {
                return longType;
            }

            public void setLongType(Long longType) {
                this.longType = longType;
            }

            public long getPrimitiveLong() {
                return primitiveLong;
            }

            public void setPrimitiveLong(long primitiveLong) {
                this.primitiveLong = primitiveLong;
            }

            public BigDecimal getBigDecimalType() {
                return bigDecimalType;
            }

            public void setBigDecimalType(BigDecimal bigDecimalType) {
                this.bigDecimalType = bigDecimalType;
            }

            public Boolean getBooleanType() {
                return booleanType;
            }

            public void setBooleanType(Boolean booleanType) {
                this.booleanType = booleanType;
            }

            public boolean isPrimitiveBoolean() {
                return primitiveBoolean;
            }

            public void setPrimitiveBoolean(boolean primitiveBoolean) {
                this.primitiveBoolean = primitiveBoolean;
            }

            @Temporal(TemporalType.TIMESTAMP)
            public Date getDateTimestampType() {
                return dateTimestampType;
            }

            public void setDateTimestampType(Date dateTimestampType) {
                this.dateTimestampType = dateTimestampType;
            }

            public Date getDateType() {
                return dateType;
            }

            public void setDateType(Date dateType) {
                this.dateType = dateType;
            }
            
            public Timestamp getTimestampType() {
                return timestampType;
            }

            public void setTimestampType(Timestamp timestampType) {
                this.timestampType = timestampType;
            }

            public LocalDate getLocalDateType() { return localDateType; }

            public void setLocalDateType(LocalDate localDateType) { this.localDateType = localDateType; }

            public LocalDateTime getLocalDateTimeType() { return localDateTimeType; }

            public void setLocalDateTimeType(LocalDateTime localDateTimeType) { this.localDateTimeType = localDateTimeType; }

            public byte[] getByteArray() {
                return byteArray;
            }

            public void setByteArray(byte[] byteArray) {
                this.byteArray = byteArray;
            }

            public List<String> getListType() {
                return listType;
            }

            public void setListType(List<String> listType) {
                this.listType = listType;
            }

            public int[] getIntArray() {
                return intArray;
            }

            public void setIntArray(int[] intArray) {
                this.intArray = intArray;
            }

            public char getInvalid() {
                return invalid;
            }

            public void setInvalid(char invalid) {
                this.invalid = invalid;
            }

            public String getNoSetter() {
                return noSetter;
            }
        }


        @Test
        public void stringType() {
            SqlRow row = createEmptySqlRow();
            row.put("STRING_TYPE", "hoge値");
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getStringType(), is("hoge値"));
        }

        @Test
        public void shortObjectType() {
            SqlRow row = createEmptySqlRow();
            {
                row.put("SHORT_TYPE", Short.MIN_VALUE);
                Hoge entity = EntityUtil.createEntity(Hoge.class, row);
                assertThat(entity.getShortType(), is(Short.MIN_VALUE));
            }
            row.put("SHORT_TYPE", null);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getShortType(), is(nullValue()));
        }

        @Test
        public void primitiveShort() {
            SqlRow row = createEmptySqlRow();
            row.put("PRIMITIVE_SHORT", Short.MAX_VALUE);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getPrimitiveShort(), is(Short.MAX_VALUE));
        }

        @Test
        public void integerType() {
            SqlRow row = createEmptySqlRow();
            {
                row.put("INTEGER_TYPE", Integer.MAX_VALUE);
                Hoge entity = EntityUtil.createEntity(Hoge.class, row);
                assertThat(entity.getIntegerType(), is(Integer.MAX_VALUE));
            }
            row.put("INTEGER_TYPE", null);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getIntegerType(), is(nullValue()));
        }

        @Test
        public void primitiveInt() {
            SqlRow row = createEmptySqlRow();
            row.put("PRIMITIVE_INT", Integer.MIN_VALUE);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getPrimitiveInt(), is(Integer.MIN_VALUE));
        }

        @Test
        public void longType() {
            SqlRow row = createEmptySqlRow();
            {
                row.put("LONG_TYPE", Long.MAX_VALUE);
                Hoge entity = EntityUtil.createEntity(Hoge.class, row);
                assertThat(entity.getLongType(), is(Long.MAX_VALUE));
            }
            row.put("LONG_TYPE", null);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getLongType(), is(nullValue()));
        }

        @Test
        public void primitiveLong() {
            SqlRow row = createEmptySqlRow();
            row.put("PRIMITIVE_LONG", Long.MIN_VALUE);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getPrimitiveLong(), is(Long.MIN_VALUE));
        }

        @Test
        public void bigDecimalType() {
            SqlRow row = createEmptySqlRow();
            row.put("BIG_DECIMAL_TYPE", new BigDecimal("1.1"));
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getBigDecimalType(), is(new BigDecimal("1.1")));
        }

        @Test
        public void booleanType_boolean() {
            SqlRow row = createEmptySqlRow();
            {
                row.put("booleanType", Boolean.TRUE);
                Hoge entity = EntityUtil.createEntity(Hoge.class, row);
                assertThat(entity.getBooleanType(), is(true));
            }
            {
                row.put("booleanType", Boolean.FALSE);
                Hoge entity = EntityUtil.createEntity(Hoge.class, row);
                assertThat(entity.getBooleanType(), is(false));
            }
            {
                row.put("booleanType", null);
                Hoge entity = EntityUtil.createEntity(Hoge.class, row);
                assertThat(entity.getBooleanType(), is(nullValue()));
            }
        }

        @Test
        public void booleanType_number() {
            SqlRow row = createEmptySqlRow();
            {
                row.put("booleanType", BigDecimal.ONE);
                Hoge entity = EntityUtil.createEntity(Hoge.class, row);
                assertThat(entity.getBooleanType(), is(Boolean.TRUE));
            }
        }

        @Test
        public void primitiveBoolean_number() {
            SqlRow row = createEmptySqlRow();
            row.put("PRIMITIVE_BOOLEAN", BigDecimal.ZERO);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.isPrimitiveBoolean(), is(false));
        }

        @Test
        public void dateType() {
            HashMap<String, Integer> typeMap = new HashMap<>();
            typeMap.put("DATE_TYPE", Types.DATE);
            SqlRow row = new SqlRow(new HashMap<>(), typeMap);
            Date date = new Date();
            row.put("DATE_TYPE", date);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getDateType(), is(date));
        }

        @Test
        public void dateTimestampType() {
            HashMap<String, Integer> typeMap = new HashMap<>();
            typeMap.put("DATE_TIMESTAMP_TYPE", Types.TIMESTAMP);
            SqlRow row = new SqlRow(new HashMap<>(), typeMap);
            Date date = new Date();
            row.put("DATE_TIMESTAMP_TYPE", new Timestamp(date.getTime()));
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getDateTimestampType(), is(date));
        }
        
        @Test
        public void timestampType() {
            HashMap<String, Integer> typeMap = new HashMap<>();
            typeMap.put("TIMESTAMP_TYPE", Types.TIMESTAMP);
            SqlRow row = new SqlRow(new HashMap<>(), typeMap);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            row.put("TIMESTAMP_TYPE", timestamp);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getTimestampType(), is(timestamp));
        }

        @Test
        public void localDateType() {
            HashMap<String, Integer> typeMap = new HashMap<>();
            typeMap.put("LOCAL_DATE_TYPE", Types.DATE);
            SqlRow row = new SqlRow(new HashMap<>(), typeMap);
            Date date = new Date();
            row.put("LOCAL_DATE_TYPE", date);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getLocalDateType(), is(date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()));
        }

        @Test
        public void localDateTimeType() {
            HashMap<String, Integer> typeMap = new HashMap<>();
            typeMap.put("LOCAL_DATE_TIME_TYPE", Types.TIMESTAMP);
            SqlRow row = new SqlRow(new HashMap<>(), typeMap);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            row.put("LOCAL_DATE_TIME_TYPE", timestamp);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getLocalDateTimeType(), is(timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()));
        }

        @Test
        public void byteArray() {
            HashMap<String, Integer> typeMap = new HashMap<>();
            typeMap.put("BYTE_ARRAY", Types.BINARY);

            SqlRow row = new SqlRow(new HashMap<>(), typeMap);
            byte[] bytes = {0x01, 0x02};
            row.put("BYTE_ARRAY", bytes);
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getByteArray(), is(bytes));
        }

        @Test
        public void invalidType() {
            HashMap<String, Integer> typeMap = new HashMap<>();
            typeMap.put("INVALID", Types.CHAR);

            SqlRow row = new SqlRow(new HashMap<>(), typeMap);
            row.put("INVALID", 'A');

            exception.expect(RuntimeException.class);
            exception.expectMessage("INVALID");
            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
        }

        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Test
        public void noSetter() {
            SqlRow row = createEmptySqlRow();
            row.put("noSetter", "hoge");

            Hoge entity = EntityUtil.createEntity(Hoge.class, row);
            assertThat(entity.getNoSetter(), is(nullValue()));
        }

        public static class NoConstructorClass {
            private NoConstructorClass() {
            }
        }

        @Test
        public void noConstructor() {
            exception.expect(BeansException.class);
            EntityUtil.createEntity(NoConstructorClass.class, createEmptySqlRow());
        }

        public static abstract class AbstractEntity {
            public AbstractEntity() {
            }
        }

        @Test
        public void abstractEntity() {
            exception.expect(BeansException.class);
            exception.expectCause(Matchers.<Throwable>instanceOf(InstantiationException.class));
            EntityUtil.createEntity(AbstractEntity.class, createEmptySqlRow());
        }

        public static class ExceptionConstructorClass {
            public ExceptionConstructorClass() throws Exception {
                throw new Exception();
            }
        }

        @Test
        public void exceptionConstructor() {
            exception.expect(BeansException.class);
            EntityUtil.createEntity(ExceptionConstructorClass.class, createEmptySqlRow());
        }

        public static class ExceptionSetterClass {
            private String prop;
            public String getProp() {
                return prop;
            }
            public void setProp(String prop) throws Exception {
                this.prop = prop;
                throw new Exception();
            }
        }

        @Test
        public void exceptionSetter() {
            SqlRow row = createEmptySqlRow();
            row.put("prop", "hoge");

            exception.expect(BeansException.class);
            exception.expectCause(CoreMatchers.<Throwable>instanceOf(InvocationTargetException.class));
            EntityUtil.createEntity(ExceptionSetterClass.class, row);
        }

        private static SqlRow createEmptySqlRow() {
            return new SqlRow(new HashMap<>(), new HashMap<>());
        }
    }

    /**
     * エンティティのキャッシュに関するテスト
     */
    public static class CacheTest {

        public static class Entity {

            @Id
            public Long getId() {
                return 1L;
            }
        }

        @ClassRule
        public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

        @After
        public void tearDown() {
            DbConnectionContext.removeConnection();
        }

        @Test
        public void test() {
            ColumnMeta first = EntityUtil.findIdColumns(Entity.class)
                    .get(0);
            ColumnMeta second = EntityUtil.findIdColumns(Entity.class)
                    .get(0);
            assertThat("1回目と2回めで同じインスタンスがかえされること", first, sameInstance(second));

            EntityUtil.clearCache();
            ColumnMeta third = EntityUtil.findIdColumns(Entity.class)
                    .get(0);
            assertThat("キャッシュクリア後に呼び出した場合は、違うインスタンスになること", first, not(sameInstance(third)));

        }
    }
}
