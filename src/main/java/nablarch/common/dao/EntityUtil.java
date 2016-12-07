package nablarch.common.dao;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import nablarch.core.beans.BeanUtil;
import nablarch.core.beans.BeansException;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;

/**
 * エンティティに関するユーティリティクラス。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public final class EntityUtil {

    /** {@link EntityMeta}のキャッシュ */
    private static final Map<Class<?>, EntityMeta> ENTITY_META_MAP = new WeakHashMap<Class<?>, EntityMeta>();

    /** 隠蔽コンストラクタ */
    private EntityUtil() {
    }

    /**
     * エンティティクラスからテーブル名を取得する。
     *
     * @param entityClass エンティティクラス
     * @return テーブル名
     */
    public static String getTableName(final Class<?> entityClass) {
        return findEntityMeta(entityClass).getTableName();
    }

    /**
     * エンティティクラスからスキーマ名を取得する。
     * @param entityClass エンティティクラス
     * @return スキーマ名
     */
    public static String getSchemaName(Class<?> entityClass) {
        return findEntityMeta(entityClass).getSchemaName();
    }

    /**
     * エンティティクラスからスキーマ名を修飾したテーブル名("スキーマ名.テーブル名"形式)を取得する。
     * <p/>
     * スキーマを持たないテーブルの場合、テーブル名のみを返す。
     *
     * @param entityClass エンティティクラス
     * @return スキーマ名を修飾したテーブル名
     */
    public static String getTableNameWithSchema(final Class<?> entityClass) {
        final String tableName = getTableName(entityClass);
        final String schemaName = getSchemaName(entityClass);

        if (StringUtil.isNullOrEmpty(schemaName)) {
            return tableName;
        } else {
            return schemaName + '.' + tableName;
        }
    }

    /**
     * エンティティクラスからIDカラムの情報を全て取得する。
     *
     * @param entityClass エンティティクラス
     * @return IDカラム情報
     */
    public static List<ColumnMeta> findIdColumns(final Class<?> entityClass) {
        return findEntityMeta(entityClass).getIdColumns();
    }

    /**
     * エンティティからIDカラムの情報と、その値を全て取得する。
     * <p/>
     * 値は{@link ColumnMeta#getJdbcType()}の型に変換されて返される。
     *
     * @param <T> エンティティクラスの型
     * @param entity エンティティオブジェクト
     * @return IDカラム情報と値
     */
    public static <T> Map<ColumnMeta, Object> findIdColumns(final T entity) {
        assert (entity != null);
        final Map<ColumnMeta, Object> idColumns = new LinkedHashMap<ColumnMeta, Object>();
        for (ColumnMeta meta : findEntityMeta(entity.getClass()).getIdColumns()) {
            idColumns.put(meta, BeanUtil.getProperty(entity, meta.getPropertyName(), meta.getJdbcType()));
        }
        return idColumns;
    }

    /**
     * エンティティクラスから全カラムの情報を取得する。
     *
     * @param entityClass エンティティクラス
     * @return 全カラムの情報
     */
    public static List<ColumnMeta> findAllColumns(final Class<?> entityClass) {
        return findEntityMeta(entityClass).getAllColumns();
    }

    /**
     * エンティティから全カラムの情報と、その値を取得する。
     * <p/>
     * 値は{@link ColumnMeta#getJdbcType()}の型に変換されて返される。
     *
     * @param <T> エンティティクラスの型
     * @param entity エンティティ
     * @return 全カラムの情報と値
     */
    public static <T> Map<ColumnMeta, Object> findAllColumns(final T entity) {
        assert (entity != null);
        final Map<ColumnMeta, Object> columns = new LinkedHashMap<ColumnMeta, Object>();

        for (ColumnMeta meta : findEntityMeta(entity.getClass()).getAllColumns()) {
            columns.put(meta, BeanUtil.getProperty(entity, meta.getPropertyName(), meta.getJdbcType()));
        }
        return columns;
    }

    /**
     * バージョンカラムの情報を取得する。
     * <p/>
     * バージョンカラムが定義されていない場合は{@code null}を返す。
     *
     * @param <T> エンティティクラスの型
     * @param entity エンティティ
     * @return バージョン情報を保持するカラムの情報
     */
    public static <T> ColumnMeta findVersionColumn(final T entity) {
        assert (entity != null);
        return findEntityMeta(entity.getClass()).getVersionColumn();
    }

    /**
     * エンティティから{@link javax.persistence.GeneratedValue}が設定されたカラムを取得する。
     * <p/>
     * 採番対象のカラムが定義されていない場合は{@code null}を返す。
     *
     * @param <T> エンティティクラスの型
     * @param entity エンティティ
     * @return 採番対象のカラムの情報
     */
    public static <T> ColumnMeta findGeneratedValueColumn(T entity) {
        assert (entity != null);
        return findGeneratedValueColumn(entity.getClass());
    }

    /**
     * エンティティクラスから{@link javax.persistence.GeneratedValue}が設定されたカラムを取得する。
     * <p/>
     * 採番対象のカラムが定義されていない場合は{@code null}を返す。
     *
     * @param <T> エンティティクラスの型
     * @param entityClass エンティティクラス
     * @return 採番対象のカラムの情報
     */
    public static <T> ColumnMeta findGeneratedValueColumn(Class<T> entityClass) {
        return findEntityMeta(entityClass).getGeneratedValueColumn();
    }

    /**
     * 検索結果を元にエンティティオブジェクトを生成する。
     *
     * @param <T> エンティティクラスの型
     * @param entityClass 生成するエンティティのクラス
     * @param row 検索結果の1レコード
     * @return エンティティオブジェクト
     * @throws RuntimeException エンティティクラスのプロパティにサポート外の型が定義されている場合
     * @throws BeansException エンティティオブジェクトの生成に失敗した場合
     */
    public static <T> T createEntity(final Class<T> entityClass, final SqlRow row) {
        try {
            final T entity = entityClass.newInstance();
            final EntityMeta entityMeta = findEntityMeta(entityClass);
            for (ColumnMeta meta : entityMeta.getAllColumns()) {
                if (!row.containsKey(meta.getName())) {
                    continue;
                }

                Object value = row.getObject(meta.getName(), meta.getPropertyType());
                BeanUtil.setProperty(entity, meta.getPropertyName(), value);
            }
            return entity;
        } catch (InstantiationException e) {
            throw new BeansException(e);
        } catch (IllegalAccessException e) {
            throw new BeansException(e);
        }
    }

    /**
     * エンティティクラスに対応したエンティティ情報を取得する。
     * <p/>
     * キャッシュ上にエンティティ情報が存在する場合はその情報を返す。
     * まだキャッシュされていない場合には、エンティティ情報を生成しキャッシュに格納する。
     *
     * @param entityClass エンティティクラス
     * @return エンティティ情報
     */
    protected static synchronized EntityMeta findEntityMeta(final Class<?> entityClass) {
        EntityMeta entityMeta = ENTITY_META_MAP.get(entityClass);
        if (entityMeta == null) {
            entityMeta = new EntityMeta(entityClass);
            ENTITY_META_MAP.put(entityClass, entityMeta);
        }
        return entityMeta;
    }

    /**
     * キャッシュ情報をクリアする。
     */
    public static synchronized void clearCache() {
        ENTITY_META_MAP.clear();
    }
}

