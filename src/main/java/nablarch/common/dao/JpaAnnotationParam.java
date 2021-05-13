package nablarch.common.dao;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.EnumMap;
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
import jakarta.persistence.TableGenerator;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;

import nablarch.core.util.StringUtil;

/**
 * アノテーションの設定を取得して保持するクラス。
 *
 * @author sioiri
 * @author Ryota Yoshinouchi
 */
class JpaAnnotationParam {

    /** 他Entityへの参照を保持することを表すアノテーションのリスト */
    @SuppressWarnings("unchecked")
    private static final List<Class<? extends Annotation>> JOIN_COLUMN_ANNOTATIONS =
            Arrays.asList(JoinColumn.class, OneToMany.class, ManyToOne.class, ManyToMany.class, OneToOne.class);

    /** {@link TemporalType}と{@link Class}との対応表 */
    private static final Map<TemporalType, Class<?>> TEMPORAL_TYPE_MAP = new EnumMap<TemporalType, Class<?>>(
            TemporalType.class);

    static {
        TEMPORAL_TYPE_MAP.put(TemporalType.DATE, java.sql.Date.class);
        TEMPORAL_TYPE_MAP.put(TemporalType.TIME, Time.class);
        TEMPORAL_TYPE_MAP.put(TemporalType.TIMESTAMP, Timestamp.class);
    }

    /** 採番カラムの情報 */
    private final GeneratedValueMetaData generatedValueMetaData;

    /** プロパティ名 */
    private final String name;

    /** 他Entityへの参照を保持するプロパティかどうか */
    private final boolean isJoinColumn;

    /** プロパティのタイプ */
    private final Class<?> propertyType;

    /** JDBCタイプ */
    private final Class<?> jdbcType;

    /** カラム名 */
    private final String columnName;

    /** 永続化対象カラムかどうか */
    private final boolean isTransientColumn;

    /** 主キーカラムかどうか */
    private final boolean isIdColumn;

    /** バージョンカラムかどうか */
    private final boolean isVersionColumn;

    /**
     * コンストラクタ。
     *
     * @param tableName テーブル名
     * @param propertyDescriptor プロパティの情報
     * @param annotations アノテーションのリスト
     */
    JpaAnnotationParam(final String tableName, final PropertyDescriptor propertyDescriptor, final Annotation[] annotations) {
        name = propertyDescriptor.getName();
        isJoinColumn = isJoinColumn(annotations);
        propertyType = propertyDescriptor.getPropertyType();
        jdbcType = getJdbcType(annotations, propertyDescriptor);
        columnName = getColumnName(annotations, propertyDescriptor);
        isTransientColumn = getAnnotation(annotations, Transient.class) != null;
        isIdColumn = getAnnotation(annotations, Id.class) != null;
        isVersionColumn = getAnnotation(annotations, Version.class) != null;
        generatedValueMetaData = createGeneratedValueMetaData(tableName, annotations);
    }

    /**
     * 他Entityへの参照を保持するカラムかどうかをアノテーションを元に判定する。
     * <p>
     * アノテーション情報に{@link #JOIN_COLUMN_ANNOTATIONS}のアノテーションが1つでも含まれる場合は、
     * 他Entityへの参照を保持するカラムと判断する。
     *
     * @param annotations アノテーション情報
     * @return 他Entityへの参照を保持するカラムの場合{@code true}
     */
    private static boolean isJoinColumn(final Annotation... annotations) {
        for (final Annotation annotation : annotations) {
            if (JOIN_COLUMN_ANNOTATIONS.contains(annotation.annotationType())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 他Entityへの参照を保持するためのカラムか否か。
     *
     * @return 他Entityへの参照を保持するカラムの場合はtrue
     */
    boolean isJoinColumn() {
        return isJoinColumn;
    }

    /**
     * プロパティ名を返す。
     *
     * @return プロパティ名
     */
    String getName() {
        return name;
    }

    /**
     * プロパティのタイプを返す。
     *
     * @return プロパティのタイプ
     */
    Class<?> getPropertyType() {
        return propertyType;
    }

    /**
     * JDBCタイプを返す。
     * <p>
     * {@link Temporal}アノテーションが設定されている場合は、{@link Temporal#value()}に対応した型を返す。
     * それ以外の場合は{@link PropertyDescriptor#getPropertyType()} を返す。
     *
     * @param annotations アノテーション情報
     * @param propertyDescriptor PropertyDescriptor
     * @return JDBCタイプ
     */
    private static Class<?> getJdbcType(final Annotation[] annotations, final PropertyDescriptor propertyDescriptor) {
        final Temporal temporal = getAnnotation(annotations, Temporal.class);
        if (temporal == null) {
            return propertyDescriptor.getPropertyType();
        }
        return TEMPORAL_TYPE_MAP.get(temporal.value());
    }

    /**
     * JDBCタイプを返す。
     *
     * @return JDBCタイプ。
     */
    Class<?> getJdbcType() {
        return jdbcType;
    }

    /**
     * カラム名を返す。
     * <p>
     * {@link Column}アノテーションが存在する場合は、{@link Column#name()}をカラム名とする。
     * 存在しない場合は、{@link PropertyDescriptor#getPropertyType() プロパティ名}を元にカラム名を導出する。
     *
     * @param annotations アノテーション情報
     * @param propertyDescriptor PropertyDescriptor
     * @return カラム名
     */
    private static String getColumnName(final Annotation[] annotations, final PropertyDescriptor propertyDescriptor) {
        final Column column = getAnnotation(annotations, Column.class);
        if (column != null && StringUtil.hasValue(column.name())) {
            return column.name();
        } else {
            return NamingConversionUtil.deCamelize(propertyDescriptor.getName());
        }
    }

    /**
     * カラム名を返す。
     *
     * @return カラム名
     */
    String getColumnName() {
        return columnName;
    }
    
    /**
     * 永続化対象外のカラムか否かを返す。
     *
     * @return 永続化対象外のカラムの場合{@code true}
     */
    boolean isTransient() {
        return isTransientColumn;
    }

    /**
     * IDカラムか否かを返す。
     *
     * @return IDカラムの場合{@code true}
     */
    boolean isIdColumn() {
        return isIdColumn;
    }

    /**
     * バージョンカラムか否かを返す。
     *
     * @return バージョンカラムの場合{@code true}
     */
    boolean isVersionColumn() {
        return isVersionColumn;
    }

    /**
     * 指定されたアノテーションクラスを返す。
     *
     * @param <T> アノテーションの型
     * @param annotations アノテーション情報
     * @param annotationClassName アノテーションクラス
     * @return アノテーションクラス。指定されたアノテーションクラスが存在しない場合はnull
     */
    @SuppressWarnings("unchecked")
    private static <T extends Annotation> T getAnnotation(final Annotation[] annotations,
            final Class<T> annotationClassName) {
        for (final Annotation annotation : annotations) {
            if (annotation.annotationType()
                          .equals(annotationClassName)) {
                return (T) annotation;
            }
        }
        return null;
    }

    /**
     * IDジェネレータの名前を取得する。
     *
     * @return IDジェネレータの名前 (シーケンスならシーケンス名 / 発番テーブルなら発番テーブルのキー名)。指定されていない場合はnullを返す。
     */
    String getGeneratorName() {
        return generatedValueMetaData != null ? generatedValueMetaData.generatorName : null;
    }

    /**
     * IDジェネレータのタイプを取得する。
     *
     * @return IDジェネレータのタイプ。指定されていない場合はnullを返す。
     */
    GenerationType getGenerationType() {
        return generatedValueMetaData != null ? generatedValueMetaData.generationType : null;
    }

    /**
     * {@link GeneratedValueMetaData}を生成する。
     *
     * @param tableName テーブル名
     * @param annotations アノテーション情報
     * @return GeneratedValueMetaData
     */
    private GeneratedValueMetaData createGeneratedValueMetaData(
            final String tableName, final Annotation[] annotations) {

        final GeneratedValue generatedValue = getAnnotation(annotations, GeneratedValue.class);
        if (generatedValue != null) {
            return new GeneratedValueMetaData(tableName, generatedValue, annotations);
        } else {
            return null;
        }
    }


    /**
     * 採番カラムの情報。
     *
     * @author hisaaki shioiri
     */
    private final class GeneratedValueMetaData {

        /** 採番タイプ */
        private final GenerationType generationType;

        /** 採番名称 */
        private final String generatorName;

        /**
         * コンストラクタ。
         *
         * @param tableName テーブル名
         * @param generatedValue GeneratedValue
         * @param annotations アノテーション情報
         */
        private GeneratedValueMetaData(
                final String tableName,
                final GeneratedValue generatedValue, final Annotation[] annotations) {

            final String generator = generatedValue.generator();
            final String columnName = getColumnName();
            switch (generatedValue.strategy()) {
                case AUTO:
                    if (getSequenceGenerator(annotations, generator) != null) {
                        generationType = GenerationType.SEQUENCE;
                        generatorName = buildSequenceName(
                                tableName, columnName, getSequenceGenerator(annotations, generator));
                    } else if (findTableGenerator(annotations, generator) != null) {
                        generationType = GenerationType.TABLE;
                        generatorName = buildTableGeneratorName(
                                tableName, columnName, findTableGenerator(annotations, generator));
                    } else {
                        generationType = GenerationType.AUTO;
                        generatorName = tableName + '_' + columnName;
                    }
                    break;
                case IDENTITY:
                    generationType = GenerationType.IDENTITY;
                    generatorName = null;
                    break;
                case SEQUENCE:
                    generationType = GenerationType.SEQUENCE;
                    generatorName = buildSequenceName(
                            tableName, columnName, getSequenceGenerator(annotations, generator));

                    break;
                case TABLE:
                    generationType = GenerationType.TABLE;
                    generatorName = buildTableGeneratorName(
                            tableName, columnName, findTableGenerator(annotations, generator));
                    break;
                default:
                    throw new IllegalArgumentException("unexpected value: " + generatedValue);
            }
        }

        /**
         * シーケンス採番を取得する。
         *
         *
         * @param annotations アノテーション情報
         * @param generator 採番名称
         * @return シーケンス採番
         */
        private SequenceGenerator getSequenceGenerator(final Annotation[] annotations,
                final String generator) {
            final SequenceGenerator sequenceGenerator = getAnnotation(annotations, SequenceGenerator.class);
            if (sequenceGenerator == null) {
                return null;
            }
            return generator.equals(sequenceGenerator.name()) ? sequenceGenerator : null;
        }

        /**
         * テーブル採番を取得する。
         *
         * @param annotations アノテーション情報
         * @param generator 採番名称
         * @return テーブル採番
         */
        private TableGenerator findTableGenerator(final Annotation[] annotations,
                final String generator) {
            final TableGenerator tableGenerator = getAnnotation(annotations, TableGenerator.class);
            if (tableGenerator == null) {
                return null;
            }
            return generator.equals(tableGenerator.name()) ? tableGenerator : null;
        }

        /**
         * シーケンス名を構築する。
         *
         * @param tableName テーブル名
         * @param columnName カラム名
         * @param sequenceGenerator シーケンス採番情報
         * @return シーケンス名
         */
        private String buildSequenceName(
                final String tableName, final String columnName, final SequenceGenerator sequenceGenerator) {
            if (sequenceGenerator != null && StringUtil.hasValue(sequenceGenerator.sequenceName())) {
                return sequenceGenerator.sequenceName();
            }
            return tableName + '_' + columnName;
        }

        /**
         * テーブル採番の識別子を構築する。
         *
         * @param tableName テーブル名
         * @param columnName カラム名
         * @param tableGenerator テーブル採番情報
         * @return テーブル採番の識別子
         */
        private String buildTableGeneratorName(
                final String tableName,
                final String columnName,
                final TableGenerator tableGenerator) {

            if (tableGenerator != null && StringUtil.hasValue(tableGenerator.pkColumnValue())) {
                return tableGenerator.pkColumnValue();
            }
            return tableName + '_' + columnName;
        }
    }


}
