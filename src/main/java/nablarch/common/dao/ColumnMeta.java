package nablarch.common.dao;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.EnumMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.TableGenerator;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.Version;

import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;

/**
 * カラムの定義情報を保持するクラス。
 *
 * @author kawaisma
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class ColumnMeta implements Serializable {

    /** {@link TemporalType}と{@link Class}との対応表 */
    private static final Map<TemporalType, Class<?>> TEMPORAL_TYPE_MAP = new EnumMap<TemporalType, Class<?>>(
            TemporalType.class);

    static {
        TEMPORAL_TYPE_MAP.put(TemporalType.DATE, java.sql.Date.class);
        TEMPORAL_TYPE_MAP.put(TemporalType.TIME, Time.class);
        TEMPORAL_TYPE_MAP.put(TemporalType.TIMESTAMP, Timestamp.class);
    }

    /** エンティティメタ情報 */
    private final EntityMeta entityMeta;

    /** カラム名 */
    private final String name;

    /** プロパティ名 */
    private final String propertyName;

    /** JDBCの型 */
    private final Class<?> jdbcType;

    /** プロパティの型 */
    private final Class<?> propertyType;

    /** 永続化対象外のプロパティを表す(対象外の場合true) */
    private final boolean isTransient;

    /** IDカラムであることを表す(IDカラムの場合true) */
    private final boolean isIdColumn;

    /** バージョンカラムであることを表す(バージョンカラムの場合true) */
    private final boolean isVersion;

    /** IDの生成タイプ */
    private final GenerationType generationType;

    /** ID生成オブジェクトを識別する名称 */
    private final String generatorName;

    /** SQL型 */
    private final Integer sqlType;

    /**
     * コンストラクタ。
     *
     * @param entityMeta エンティティ定義のメタデータ
     * @param pd プロパティ情報
     * @param sqlTypeMap カラムのSQL型(キー:カラム名、値:SQL型)
     */
    public ColumnMeta(final EntityMeta entityMeta, final PropertyDescriptor pd, Map<String, Integer> sqlTypeMap) {
        this.entityMeta = entityMeta;

        final Method getterMethod = pd.getReadMethod();
        propertyName = pd.getName();
        propertyType = pd.getPropertyType();
        name = findColumnName(pd, getterMethod);

        isTransient = isTransient(pd, getterMethod);
        isIdColumn = hasAnnotation(getterMethod, Id.class);
        isVersion = hasAnnotation(getterMethod, Version.class);
        jdbcType = convertJdbcType(getterMethod, propertyType);

        GeneratedValueMetaData generatedValueMetaData =
                new GeneratedValueMetaData(entityMeta.getTableName(), name, getterMethod);
        generationType = generatedValueMetaData.generationType;
        generatorName = generatedValueMetaData.generatorName;

        sqlType = sqlTypeMap.get(name.toUpperCase());
    }

    /**
     * JDBCタイプを取得する。
     *
     * @param method メソッド
     * @param type プロパティのタイプ
     * @return JDBCタイプ
     */
    private static Class<?> convertJdbcType(Method method, Class<?> type) {
        if (!hasAnnotation(method, Temporal.class)) {
            return type;
        }

        TemporalType temporalType = method.getAnnotation(Temporal.class).value();
        return TEMPORAL_TYPE_MAP.get(temporalType);
    }

    /**
     * メソッドにアノテーションが設定されているか否か。
     *
     * @param method メソッド
     * @param annotation アノテーションクラス
     * @return メソッドに指定のアノテーションクラスが設定されている場合はtrue
     */
    private static boolean hasAnnotation(Method method, Class<? extends Annotation> annotation) {
        return method.getAnnotation(annotation) != null;
    }

    /**
     * カラム名を取得する。
     *
     * @param pd {@link PropertyDescriptor}
     * @param method メソッド
     * @return カラム名
     */
    private static String findColumnName(PropertyDescriptor pd, Method method) {
        final Column column = method.getAnnotation(Column.class);
        if (column != null && StringUtil.hasValue(column.name())) {
            return column.name();
        } else {
            return NamingConversionUtil.deCamelize(pd.getName());
        }
    }

    /**
     * 永続化対象外のカラムか否かを判定する。
     *
     * @param pd {@link PropertyDescriptor}
     * @param method メソッド
     * @return 永続化対象外のカラムの場合{@code true}
     */
    private static boolean isTransient(PropertyDescriptor pd, Method method) {
        return hasAnnotation(method, Transient.class);
    }

    /**
     * データベースのカラム名を取得する。
     *
     * @return カラム名
     */
    public String getName() {
        return name;
    }

    /**
     * Entityクラスのプロパティ名を取得する。
     *
     * @return プロパティ名
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * JDBCでSQLにバインドするときの型を取得する。
     *
     * @return JDBCでバインドするときの型
     */
    public Class<?> getJdbcType() {
        return jdbcType;
    }

    /**
     * Entityクラスのプロパティ型を取得する。
     *
     * @return プロパティの型
     */
    public Class<?> getPropertyType() {
        return propertyType;
    }

    /**
     * プロパティが揮発性なものかどうかを取得する。
     *
     * @return 揮発性ならばtrue
     */
    protected boolean isTransient() {
        return isTransient;
    }

    /**
     * カラムがプライマリーキーを構成するかどうかを取得する。
     *
     * @return プライマリーキーを構成すればtrue
     */
    public boolean isIdColumn() {
        return isIdColumn;
    }

    /**
     * カラムが楽観排他用のバージョンを表すかどうかを取得する。
     *
     * @return バージョンを表せばtrue
     */
    public boolean isVersion() {
        return isVersion;
    }

    /**
     * 自動生成カラムか否か。
     *
     * @return 生成タイプがnullでないならばtrue
     */
    public boolean isGeneratedValue() {
        return generationType != null;
    }

    /**
     * IDジェネレータのタイプを取得する。
     *
     * @return IDジェネレータのタイプ
     */
    public GenerationType getGenerationType() {
        return generationType;
    }

    /**
     * IDジェネレータの名前を取得する。
     *
     * @return IDジェネレータの名前 (シーケンスならシーケンス名 / 発番テーブルなら発番テーブルのキー名)
     */
    public String getGeneratorName() {
        return generatorName;
    }

    /**
     * SQL型を取得する。
     *
     * @return SQL型
     */
    public Integer getSqlType() {
        return sqlType;
    }

    @Override
    public boolean equals(final Object another) {
        if (another == null || !(another instanceof ColumnMeta)) {
            return false;
        }
        final ColumnMeta anotherMeta = ColumnMeta.class.cast(another);
        return this.name.equals(anotherMeta.getName())
                && entityMeta.equals(anotherMeta.entityMeta);
    }

    @Override
    public int hashCode() {
        return name.hashCode() + entityMeta.hashCode();
    }

    /**
     * 採番カラムの情報。
     * @author hisaaki shioiri
     */
    private static final class GeneratedValueMetaData {

        /** 採番タイプ */
        private final GenerationType generationType;

        /** 採番名称 */
        private final String generatorName;

        /**
         * コンストラクタ。
         * @param tableName テーブル名
         * @param columnName カラム名
         * @param method メソッド
         */
        private GeneratedValueMetaData(String tableName, String columnName, Method method) {
            GeneratedValue generatedValue = method.getAnnotation(GeneratedValue.class);
            if (generatedValue != null) {
                String generator = generatedValue.generator();

                switch (generatedValue.strategy()) {
                    case AUTO:
                        if (findSequenceGenerator(generator, method) != null) {
                            generationType = GenerationType.SEQUENCE;
                            generatorName = buildSequenceName(
                                    tableName, columnName, findSequenceGenerator(generator, method));
                        } else if (findTableGenerator(generator, method) != null) {
                            generationType = GenerationType.TABLE;
                            generatorName = buildTableGeneratorName(
                                    tableName, columnName, findTableGenerator(generator, method));
                        } else {
                            generationType = GenerationType.AUTO;
                            generatorName = tableName + '_' + columnName;
                        }
                        return;
                    case IDENTITY:
                        generationType = GenerationType.IDENTITY;
                        generatorName = null;

                        return;
                    case SEQUENCE:
                        generationType = GenerationType.SEQUENCE;
                        generatorName = buildSequenceName(
                                tableName, columnName, findSequenceGenerator(generator, method));

                        return;
                    case TABLE:
                        generationType = GenerationType.TABLE;
                        generatorName = buildTableGeneratorName(
                                tableName, columnName, findTableGenerator(generator, method));
                        return;
                }
            }
            generationType = null;
            generatorName = null;
        }

        /**
         * シーケンス採番を取得する。
         * @param generator 採番名称
         * @param method メソッド
         * @return シーケンス採番
         */
        private static SequenceGenerator findSequenceGenerator(String generator, Method method) {
            SequenceGenerator sequenceGenerator = method.getAnnotation(SequenceGenerator.class);
            if (sequenceGenerator == null) {
                return null;
            }
            return generator.equals(sequenceGenerator.name()) ? sequenceGenerator : null;
        }

        /**
         * テーブル採番を取得する。
         * @param generator 採番名称
         * @param method メソッド
         * @return テーブル採番
         */
        private static TableGenerator findTableGenerator(String generator, Method method) {
            TableGenerator tableGenerator = method.getAnnotation(TableGenerator.class);
            if (tableGenerator == null) {
                return null;
            }
            return generator.equals(tableGenerator.name()) ? tableGenerator : null;
        }

        /**
         * シーケンス名を構築する。
         * @param tableName テーブル名
         * @param columnName カラム名
         * @param sequenceGenerator シーケンス採番情報
         * @return シーケンス名
         */
        private static String buildSequenceName(
                String tableName, String columnName, SequenceGenerator sequenceGenerator) {
            if (sequenceGenerator != null && StringUtil.hasValue(sequenceGenerator.sequenceName())) {
                return sequenceGenerator.sequenceName();
            }
            return tableName + '_' + columnName;
        }

        /**
         * テーブル採番の識別子を構築する。
         * @param tableName テーブル名
         * @param columnName カラム名
         * @param tableGenerator テーブル採番情報
         * @return テーブル採番の識別子
         */
        private static String buildTableGeneratorName(
                String tableName, String columnName, TableGenerator tableGenerator) {
            if (tableGenerator != null && StringUtil.hasValue(tableGenerator.pkColumnValue())) {
                return tableGenerator.pkColumnValue();
            }
            return tableName + '_' + columnName;
        }
    }
}
