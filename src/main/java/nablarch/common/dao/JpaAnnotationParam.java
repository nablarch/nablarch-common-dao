package nablarch.common.dao;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.TableGenerator;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;
import javax.persistence.Version;

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


    /** カラムに対応したプロパティの情報 */
    private final PropertyDescriptor propertyDescriptor;
    /** その属性に設定されたアノテーションのリスト */
    private final Annotation[] annotations;
    /** 採番カラムの情報 */
    private final GeneratedValueMetaData generatedValueMetaData;

    /**
     * コンストラクタ。
     *
     * @param tableName テーブル名
     * @param propertyDescriptor プロパティの情報
     * @param annotations アノテーションのリスト
     */
    JpaAnnotationParam(final String tableName, final PropertyDescriptor propertyDescriptor, final Annotation[] annotations) {
        this.propertyDescriptor = propertyDescriptor;
        this.annotations = annotations;
        final GeneratedValue generatedValue = getAnnotation(GeneratedValue.class);
        if (generatedValue != null) {
            generatedValueMetaData = new GeneratedValueMetaData(tableName, generatedValue);
        } else {
            generatedValueMetaData = null;
        }
    }

    /**
     * 他Entityへの参照を保持するためのカラムか否か。
     *
     * @return 他Entityへの参照を保持するカラムの場合はtrue
     */
    boolean isJoinColumn() {
        for (final Annotation annotation : annotations) {
            if (JOIN_COLUMN_ANNOTATIONS.contains(annotation.annotationType())) {
                return true;
            }
        }
        return false;
    }

    /**
     * プロパティ名を返す。
     *
     * @return プロパティ名
     */
    String getName() {
        return propertyDescriptor.getName();
    }

    /**
     * プロパティのタイプを返す。
     *
     * @return プロパティのタイプ
     */
    Class<?> getPropertyType() {
        return propertyDescriptor.getPropertyType();
    }

    /**
     * JDBCタイプを返す。
     * <p>
     * {@link Temporal}アノテーションが設定されている場合は、
     * {@link Temporal#value()}に対応した型を返す。
     * それ以外の場合は、{@link #getPropertyType()} を返す。
     *
     * @return JDBCタイプ。
     */
    Class<?> getJdbcType() {
        final Temporal temporal = getAnnotation(Temporal.class);
        if (temporal == null) {
            return getPropertyType();
        }
        return TEMPORAL_TYPE_MAP.get(temporal.value());
    }


    /**
     * カラム名を返す。
     *
     * @return カラム名
     */
    String getColumnName() {
        final Column column = getAnnotation(Column.class);
        if (column != null && StringUtil.hasValue(column.name())) {
            return column.name();
        } else {
            return NamingConversionUtil.deCamelize(propertyDescriptor.getName());
        }
    }

    /**
     * 永続化対象外のカラムか否かを返す。
     *
     * @return 永続化対象外のカラムの場合{@code true}
     */
    boolean isTransient() {
        return getAnnotation(Transient.class) != null;
    }

    /**
     * IDカラムか否かを返す。
     *
     * @return IDカラムの場合{@code true}
     */
    boolean isIdColumn() {
        return getAnnotation(Id.class) != null;
    }

    /**
     * バージョンカラムか否かを返す。
     *
     * @return バージョンカラムの場合{@code true}
     */
    boolean isVersionColumn() {
        return getAnnotation(Version.class) != null;
    }

    /**
     * 指定されたアノテーションクラスを返す。
     *
     * @param annotationClassName アノテーションクラス
     * @param <T> アノテーションの型
     * @return アノテーションクラス。指定されたアノテーションクラスが存在しない場合はnull
     */
    @SuppressWarnings("unchecked")
    private <T extends Annotation> T getAnnotation(final Class<T> annotationClassName) {
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
         */
        private GeneratedValueMetaData(
                final String tableName,
                final GeneratedValue generatedValue) {

            final String generator = generatedValue.generator();
            final String columnName = getColumnName();
            switch (generatedValue.strategy()) {
                case AUTO:
                    if (getSequenceGenerator(generator) != null) {
                        generationType = GenerationType.SEQUENCE;
                        generatorName = buildSequenceName(
                                tableName, columnName, getSequenceGenerator(generator));
                    } else if (findTableGenerator(generator) != null) {
                        generationType = GenerationType.TABLE;
                        generatorName = buildTableGeneratorName(
                                tableName, columnName, findTableGenerator(generator));
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
                            tableName, columnName, getSequenceGenerator(generator));

                    break;
                case TABLE:
                    generationType = GenerationType.TABLE;
                    generatorName = buildTableGeneratorName(
                            tableName, columnName, findTableGenerator(generator));
                    break;
                default:
                    throw new IllegalArgumentException("unexpected value: " + generatedValue);
            }
        }

        /**
         * シーケンス採番を取得する。
         *
         * @param generator 採番名称
         * @return シーケンス採番
         */
        private SequenceGenerator getSequenceGenerator(final String generator) {
            final SequenceGenerator sequenceGenerator = getAnnotation(SequenceGenerator.class);
            if (sequenceGenerator == null) {
                return null;
            }
            return generator.equals(sequenceGenerator.name()) ? sequenceGenerator : null;
        }

        /**
         * テーブル採番を取得する。
         *
         * @param generator 採番名称
         * @return テーブル採番
         */
        private TableGenerator findTableGenerator(final String generator) {
            final TableGenerator tableGenerator = getAnnotation(TableGenerator.class);
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
