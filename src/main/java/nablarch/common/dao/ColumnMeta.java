package nablarch.common.dao;

import java.io.Serializable;

import javax.persistence.GenerationType;

import nablarch.core.util.annotation.Published;

/**
 * カラムの定義情報を保持するクラス。
 *
 * @author kawaisma
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class ColumnMeta implements Serializable {

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

    /**
     * コンストラクタ。
     *  @param entityMeta エンティティ定義のメタデータ
     * @param columnDefinition プロパティ情報
     */
    public ColumnMeta(final EntityMeta entityMeta, final ColumnDefinition columnDefinition) {
        this.entityMeta = entityMeta;

        propertyName = columnDefinition.getName();
        propertyType = columnDefinition.getPropertyType();
        name = columnDefinition.getColumnName();

        isTransient = columnDefinition.isTransient();
        isIdColumn = columnDefinition.isIdColumn();
        isVersion = columnDefinition.isVersionColumn();
        jdbcType = columnDefinition.getJdbcType();

        generationType = columnDefinition.getGenerationType();
        generatorName = columnDefinition.getGeneratorName();

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

}

