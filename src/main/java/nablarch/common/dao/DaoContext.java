package nablarch.common.dao;

import java.util.List;

import jakarta.persistence.OptimisticLockException;

import nablarch.core.util.annotation.Published;

/**
 * {@link UniversalDao}の実行コンテキスト。
 * <p/>
 * ページネーションのためのページ数などを状態としてもつ必要があるので、
 * このコンテキストを介してSQLの実行処理が行われる。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public interface DaoContext {

    /**
     * プライマリーキーによる検索を行う。
     *
     * @param <T> エンティティクラスの型
     * @param entityClass エンティティクラス
     * @param id プライマリーキー (複合キーの場合は定義順)
     * @return エンティティオブジェクト
     */
    <T> T findById(Class<T> entityClass, Object... id);

    /**
     * プライマリーキーによる検索を行う。
     *
     * @param <T> エンティティクラスの型
     * @param entityClass エンティティクラス
     * @param id プライマリーキー (複合キーの場合は定義順)
     * @return エンティティオブジェクト。0件の場合はnull。
     */
    <T> T findByIdOrNull(Class<T> entityClass, Object... id);

    /**
     * 全件の検索を行う。
     *
     * @param <T> エンティティクラスの型
     * @param entityClass エンティティクラス
     * @return 検索結果リスト。0件の場合は空リスト。
     */
    @Published
    <T> EntityList<T> findAll(Class<T> entityClass);

    /**
     * SQL_IDをもとにバインド変数を展開して検索処理を行う。
     *
     * @param <T> 検索結果をマッピングするBeanクラスの型
     * @param entityClass 検索結果をマッピングするBeanクラス
     * @param sqlId SQL_ID
     * @param params バインド変数
     * @return 検索結果リスト。0件の場合は空リスト。
     */
    @Published
    <T> EntityList<T> findAllBySqlFile(Class<T> entityClass, String sqlId, Object params);

    /**
     * SQL_IDをもとに検索を行う。
     *
     * @param <T> 検索結果をマッピングするBeanクラスの型
     * @param entityClass 検索結果をマッピングするBeanクラス
     * @param sqlId SQL_ID
     * @return 検索結果リスト。0件の場合は空リスト。
     */
    @Published
    <T> EntityList<T> findAllBySqlFile(Class<T> entityClass, String sqlId);

    /**
     * SQL_IDをもとに1件検索を行う。
     *
     * @param <T> 検索結果をマッピングするBeanクラスの型
     * @param entityClass 検索結果をマッピングするBeanクラス
     * @param sqlId SQL_ID
     * @param params バインド変数
     * @return エンティティオブジェクト
     */
    <T> T findBySqlFile(Class<T> entityClass, String sqlId, Object params);

    /**
     * SQL_IDをもとに1件検索を行う。
     *
     * @param <T> 検索結果をマッピングするBeanクラスの型
     * @param entityClass 検索結果をマッピングするBeanクラス
     * @param sqlId SQL_ID
     * @param params バインド変数
     * @return エンティティオブジェクト。0件の場合はnull。
     */
    <T> T findBySqlFileOrNull(Class<T> entityClass, String sqlId, Object params);

    /**
     * SQL_IDをもとに結果件数を取得する。
     *
     * @param <T> エンティティクラスの型
     * @param entityClass エンティティクラス
     * @param sqlId SQL_ID
     * @param params バインド変数
     * @return 件数
     */
    <T> long countBySqlFile(Class<T> entityClass, String sqlId, Object params);

    /**
     * エンティティオブジェクトを元に更新処理を行う。
     * <p/>
     * エンティティの主キーが更新条件となる。
     *
     * @param <T> エンティティクラスの型
     * @param entity エンティティオブジェクト
     * @return 更新件数
     * @throws OptimisticLockException バージョン不一致で更新対象が存在しない場合
     */
    <T> int update(T entity) throws OptimisticLockException;

    /**
     * エンティティオブジェクトの情報を元に一括更新を行う。
     * <p/>
     * {@link #update(Object)}とは異なり、一括更新処理ではバージョン不一致チェックは行わない。
     * 例えば、バージョン番号が変更になっていた場合はそのレコードのみ更新されずに処理は正常に終了する。
     * バージョン番号のチェックを必要とする場合には、{@link #update(Object)}を使用すること。
     *
     * @param entities 更新対象のエンティティリスト
     * @param <T> エンティティクラスの型
     */
    <T> void batchUpdate(List<T> entities);

    /**
     * エンティティオブジェクトを元に登録処理を行う。
     *
     * @param <T> エンティティクラスの型
     * @param entity エンティティオブジェクト
     */
    <T> void insert(T entity);


    /**
     * エンティティオブジェクトの情報を一括で登録する。
     * @param entities エンティティリスト
     * @param <T> エンティティクラスの型
     */
    <T> void batchInsert(List<T> entities);

    /**
     * エンティティオブジェクトを元に削除処理を行う。
     * <p/>
     * エンティティの主キーが削除条件となる。
     *
     * @param <T> エンティティクラスの型
     * @param entity エンティティオブジェクト
     * @return 削除件数
     */
    <T> int delete(T entity);

    /**
     * エンティティオブジェクトを元に一括削除処理を行う。
     * <p/>
     * エンティティの主キーが削除条件となる。
     *
     * @param entities エンティティリスト
     * @param <T> エンティティクラスの型
     */
    <T> void batchDelete(List<T> entities);

    /**
     * ページングの何ページ目を検索するかを指定する。
     *
     * @param page ページ番号(1-origin)
     * @return DaoContextがそのまま返る。
     */
    DaoContext page(long page);

    /**
     * ページングの1ページにつき何件表示するかを指定する。
     *
     * @param per ページ内表示件数
     * @return DaoContextがそのまま返る。
     */
    DaoContext per(long per);

    /**
     * 検索結果の取得を遅延させる。
     *
     * @return DaoContextがそのまま返る。
     */
    DaoContext defer();
}
