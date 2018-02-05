package nablarch.common.dao;

import javax.persistence.GenerationType;

import nablarch.core.util.annotation.Published;

/**
 * カラムの定義情報を保持するクラス。
 *
 * @author kawaisma
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class ColumnMeta {

    /** エンティティメタ情報 */
    private final EntityMeta entityMeta;

    /** アノテーションの設定 */
    private final JpaAnnotationParam jpaAnnotationParam;

    /**
     * コンストラクタ。
     * @param entityMeta エンティティ定義のメタデータ
     * @param jpaAnnotationParam プロパティ情報
     */
    public ColumnMeta(final EntityMeta entityMeta, final JpaAnnotationParam jpaAnnotationParam) {
        this.entityMeta = entityMeta;
        this.jpaAnnotationParam = jpaAnnotationParam;
    }

    /**
     * データベースのカラム名を取得する。
     *
     * @return カラム名
     */
    public String getName() {
        return jpaAnnotationParam.getColumnName();
    }

    /**
     * Entityクラスのプロパティ名を取得する。
     *
     * @return プロパティ名
     */
    public String getPropertyName() {
        return jpaAnnotationParam.getName();
    }

    /**
     * JDBCでSQLにバインドするときの型を取得する。
     *
     * @return JDBCでバインドするときの型
     */
    public Class<?> getJdbcType() {
        return jpaAnnotationParam.getJdbcType();
    }

    /**
     * Entityクラスのプロパティ型を取得する。
     *
     * @return プロパティの型
     */
    public Class<?> getPropertyType() {
        return jpaAnnotationParam.getPropertyType();
    }

    /**
     * プロパティが揮発性なものかどうかを取得する。
     *
     * @return 揮発性ならばtrue
     */
    protected boolean isTransient() {
        return jpaAnnotationParam.isTransient();
    }

    /**
     * カラムがプライマリーキーを構成するかどうかを取得する。
     *
     * @return プライマリーキーを構成すればtrue
     */
    public boolean isIdColumn() {
        return jpaAnnotationParam.isIdColumn();
    }

    /**
     * カラムが楽観排他用のバージョンを表すかどうかを取得する。
     *
     * @return バージョンを表せばtrue
     */
    public boolean isVersion() {
        return jpaAnnotationParam.isVersionColumn();
    }

    /**
     * 自動生成カラムか否か。
     *
     * @return 生成タイプがnullでないならばtrue
     */
    public boolean isGeneratedValue() {
        return jpaAnnotationParam.getGenerationType() != null;
    }

    /**
     * IDジェネレータのタイプを取得する。
     *
     * @return IDジェネレータのタイプ
     */
    public GenerationType getGenerationType() {
        return jpaAnnotationParam.getGenerationType();
    }

    /**
     * IDジェネレータの名前を取得する。
     *
     * @return IDジェネレータの名前 (シーケンスならシーケンス名 / 発番テーブルなら発番テーブルのキー名)
     */
    public String getGeneratorName() {
        return jpaAnnotationParam.getGeneratorName();
    }

    @Override
    public boolean equals(final Object another) {
        if (another == null || !(another instanceof ColumnMeta)) {
            return false;
        }
        final ColumnMeta anotherMeta = ColumnMeta.class.cast(another);
        return this.jpaAnnotationParam.getColumnName().equals(anotherMeta.getName())
                && entityMeta.equals(anotherMeta.entityMeta);
    }

    @Override
    public int hashCode() {
        return this.jpaAnnotationParam.getColumnName().hashCode() + entityMeta.hashCode();
    }

}

