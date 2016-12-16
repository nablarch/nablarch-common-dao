package nablarch.common.dao;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.persistence.JoinColumn;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import nablarch.core.beans.BeanUtil;
import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;

/**
 * Entityクラスのメタデータを保持するクラス。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class EntityMeta implements Serializable {

    /** テーブル名 */
    private final String tableName;

    /** スキーマ名 */
    private final String schemaName;

    /** カラムのメタ情報 */
    private final List<ColumnMeta> columnMetaList;

    /** ID（主キー）カラムのリスト */
    private final List<ColumnMeta> idColumns;

    /** バージョンカラム */
    private final ColumnMeta versionColumn;

    /** 採番カラム */
    private final ColumnMeta generatedValueColumn;

    /** 主キー検索が実行できるか否か */
    private boolean enableFindById = true;

    /**
     * コンストラクタ。
     *
     * @param entityClass エンティティクラス
     */
    public EntityMeta(final Class<?> entityClass) {

        tableName = findTableName(entityClass);

        schemaName = findSchemaName(entityClass);

        final PropertyDescriptor[] propertyDescriptors = BeanUtil.getPropertyDescriptors(entityClass);

        columnMetaList = new ArrayList<ColumnMeta>(propertyDescriptors.length);
        idColumns = new ArrayList<ColumnMeta>(propertyDescriptors.length);

        ColumnMeta tempVersionColumn = null;
        ColumnMeta tempGeneratedValueColumn = null;
        Map<String, Integer> sqlTypeMap = DatabaseUtil.getSqlTypeMap(schemaName, tableName);
        for (PropertyDescriptor pd : propertyDescriptors) {
            if (isJoinColumn(pd)) {
                continue;
            }
            final ColumnMeta meta = new ColumnMeta(this, pd, sqlTypeMap);
            if (!meta.isTransient()) {
                columnMetaList.add(meta);
            }
            if (meta.isIdColumn()) {
                idColumns.add(meta);
            }
            if (meta.isVersion()) {
                if (tempVersionColumn != null) {
                    throw new IllegalEntityException(
                            "version column must be single definition. class name = " + entityClass.getName());
                }
                tempVersionColumn = meta;
            }
            if (meta.isGeneratedValue()) {
                if (tempGeneratedValueColumn != null) {
                    throw new IllegalEntityException(
                            "Generated value column must be single definition. class name = " + entityClass.getName());
                }
                tempGeneratedValueColumn = meta;
            }
        }
        versionColumn = tempVersionColumn;
        generatedValueColumn = tempGeneratedValueColumn;

        try {
            sortIdColumns();
        } catch (RuntimeException e) {
            enableFindById = false;
        }
    }

    /**
     * エンティティクラスからテーブル名を取得する。
     * <p/>
     * {@link Table}アノテーションが設定されており、{@link Table#name()}が設定されている場合は、
     * その値をテーブル名とする。
     * これに該当しない場合は、エンティティクラスのクラス名をスネークケース(全て大文字)に変換した値をテーブル名とする。
     *
     * @param entityClass エンティティクラス。
     * @return テーブル名
     */
    private static String findTableName(Class<?> entityClass) {
        final Table table = entityClass.getAnnotation(Table.class);
        if (table != null && StringUtil.hasValue(table.name())) {
            return table.name();
        } else {
            return NamingConversionUtil.deCamelize(entityClass.getSimpleName());
        }
    }

    /**
     * エンティティクラスからスキーマ名を取得する。
     *
     * {@link Table#schema()}が設定されている場合、その値をスキーマ名として返す。
     * それ以外の場合は、{@code null}を返す。
     *
     * @param entityClass エンティティクラス。
     * @return スキーマ名
     */
    private static String findSchemaName(final Class<?> entityClass) {
        final Table table = entityClass.getAnnotation(Table.class);
        if (table == null) {
            return null;
        }
        final String schema = table.schema();
        return StringUtil.isNullOrEmpty(schema) ? null : schema;
    }

    /**
     * Joinカラムかどうか判定する。
     *
     * @param pd 属性のプロパティディスクリプタ
     * @return Joinカラムであればtrue
     */
    private static boolean isJoinColumn(final PropertyDescriptor pd) {
        final Method getter = pd.getReadMethod();
        return getter.getAnnotation(JoinColumn.class) != null
                || getter.getAnnotation(OneToMany.class) != null
                || getter.getAnnotation(ManyToOne.class) != null
                || getter.getAnnotation(ManyToMany.class) != null
                || getter.getAnnotation(OneToOne.class) != null;
    }

    /**
     * IDカラム(主キーカラム)のリストを返す。
     *
     * @return カラムメタデータリスト
     */
    public List<ColumnMeta> getIdColumns() {
        return new ArrayList<ColumnMeta>(idColumns);
    }

    /**
     * 全カラムのリストを返す。
     *
     * @return 全カラムメタデータリスト
     */
    public List<ColumnMeta> getAllColumns() {
        return new ArrayList<ColumnMeta>(columnMetaList);
    }

    /**
     * バージョンカラムを返す。
     *
     * @return バージョンカラム情報
     */
    public ColumnMeta getVersionColumn() {
        return versionColumn;
    }

    /**
     * 採番カラムを返す。
     * @return 採番カラム情報
     */
    public ColumnMeta getGeneratedValueColumn() {
        return generatedValueColumn;
    }

    /**
     * テーブル名を返す。
     *
     * @return テーブル名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * スキーマ名を返す。
     * @return スキーマ名
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * IDカラムから情報が取得可能か否か。
     *
     * @return IDからカラム情報が取得可能な場合true
     */
    public boolean canFindById() {
        return enableFindById;
    }

    /**
     * Primary keyの順番をデータベースの定義順にソートする。
     */
    protected void sortIdColumns() {
        final Map<String, Short> primaryKeyOrder = DatabaseUtil.getPrimaryKey(tableName);
        Collections.sort(idColumns, new Comparator<ColumnMeta>() {
            @Override
            public int compare(final ColumnMeta cm1, final ColumnMeta cm2) {
                return primaryKeyOrder.get(cm1.getName().toUpperCase())
                        - primaryKeyOrder.get(cm2.getName().toUpperCase());
            }
        });
    }

    @Override
    public boolean equals(final Object another) {
        if (another == null || !(another instanceof EntityMeta)) {
            return false;
        }
        final EntityMeta anotherMeta = EntityMeta.class.cast(another);
        return this.tableName.equals(anotherMeta.tableName);
    }

    @Override
    public int hashCode() {
        return tableName.hashCode();
    }

}

