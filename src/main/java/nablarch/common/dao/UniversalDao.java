package nablarch.common.dao;

import java.util.List;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.SystemRepository;
import nablarch.core.util.annotation.Published;

/**
 * 汎用的なDAO機能を提供するクラス。
 * <p/>
 * 以下の機能を提供する。
 * <p/>
 * <ul>
 * <li>主キーを条件にしたSELECT・UPDATE・DELETE文と、INSERT文をEntityクラスから自動生成して実行する。</li>
 * <li>SQLを実行する</li>
 * <li>検索結果をBeanにマッピングする</li>
 * <li>ページングのための検索を行う</li>
 * <li>検索時に遅延ロードを行う</li>
 * </ul>
 * <p/>
 * EntityはJPA2.0のアノテーションに準拠する。
 * <p/>
 * サポートしているものは、以下である。
 * <p/>
 * <ul>
 * <li>{@link jakarta.persistence.Entity}</li>
 * <li>{@link jakarta.persistence.Table}</li>
 * <li>{@link jakarta.persistence.Column}</li>
 * <li>{@link jakarta.persistence.Id}</li>
 * <li>{@link jakarta.persistence.Version}</li>
 * <li>{@link jakarta.persistence.Temporal}</li>
 * <li>{@link jakarta.persistence.GeneratedValue}</li>
 * <li>{@link jakarta.persistence.SequenceGenerator}</li>
 * <li>{@link jakarta.persistence.TableGenerator}</li>
 * </ul>
 * <p/>
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published
public final class UniversalDao {

    /** {@link DaoContextFactory}を{@link SystemRepository}からルックアップする際の名前 */
    private static final String DAO_CONTEXT_FACTORY = "daoContextFactory";

    /** 空のパラメータを表す定数値 */
    private static final Object[] EMPTY_PARAM = new Object[0];

    /**
     * 隠蔽コンストラクタ。
     */
    private UniversalDao() {
    }

    /**
     * {@link DaoContext}を取得する。
     *
     * {@link #DAO_CONTEXT_FACTORY}で{@link SystemRepository}上に{@link DaoContextFactory}実装が登録されている場合はそのクラスを、
     * 登録されていない場合には{@link BasicDaoContextFactory}を用いて{@link DaoContext}を生成する。
     *
     * @return DaoContext
     */
    private static DaoContext daoContext() {
        final DaoContextFactory factory = SystemRepository.get(DAO_CONTEXT_FACTORY);
        return factory != null ? factory.create() : new BasicDaoContextFactory().create();
    }

    /**
     * 主キーを指定して、1件だけエンティティを取得する。
     *
     * @param <T> エンティティクラス(戻り値の型)
     * @param entityClass エンティティクラスオブジェクト
     * @param id 条件項目(複数のキーを使う場合は、対象テーブルでのキーの定義順に引き渡す)
     * @return 取得したエンティティ
     * @throws NoDataException 検索条件に該当するレコードが存在しない場合
     * @throws IllegalStateException 対象テーブルから主キーの定義順を取得できなかった場合
     */
    public static <T> T findById(final Class<T> entityClass, final Object... id) {
        if (id.length == 1 || EntityUtil.findEntityMeta(entityClass).canFindById()) {
            return daoContext().findById(entityClass, id);
        } else {
            throw new IllegalStateException("For findById, enable to get the orders of primary keys.");
        }
    }

    /**
     * すべてのエンティティを取得する。
     *
     * @param <T> エンティティクラス(戻り値の型)
     * @param entityClass エンティティクラスオブジェクト
     * @return 取得したエンティティのリスト(該当0件の場合は空リスト)
     */
    public static <T> EntityList<T> findAll(final Class<T> entityClass) {
        return daoContext().findAll(entityClass);
    }

    /**
     * SQL_IDをもとにバインド変数を展開した上で検索し、結果Beanのリストに格納して取得する。
     * <pre>
     * {@code
     * // 検索条件を引き渡すためのBeanを設定する
     * // SQL「FIND_BY_AUTHOR」にBookエンティティのAUTHORカラムがバインド変数として記述されている場合を想定する
     * Book condition = new Book();
     * condition.setAuthor("Martin Fowler");
     *
     * EntityList<Book> books = UniversalDao.findAllBySqlFile(Book.class, "FIND_BY_AUTHOR", condition);
     * }</pre>
     * <p/>
     * 結合した表のカラムを含めて射影する場合は、単一の表とマッピングされたEntityでは結果を格納できない。
     * そのような場合は、射影したカラムと対応するプロパティを定義したBeanを引き渡す。
     *
     * @param <T> 検索結果をマッピングするBeanクラス
     * @param entityClass 検索結果をマッピングするBeanクラスオブジェクト
     * @param sqlId SQL_ID
     * @param params バインド変数(SQLファイル内のバインド変数に対応するBeanを作成し引き渡すこともできる)
     * @return 取得したBeanのリスト(該当0件の場合は空リスト)
     */
    public static <T> EntityList<T> findAllBySqlFile(
            final Class<T> entityClass, final String sqlId, final Object params) {
        return daoContext().findAllBySqlFile(entityClass, sqlId, params);
    }

    /**
     * SQL_IDをもとに検索し、結果Beanのリストに格納して取得する。
     * <p/>
     * 検索の詳細は{@link #findAllBySqlFile(Class, String, Object)}を参照すること。
     *
     * @param <T> 検索結果をマッピングするBeanクラス
     * @param entityClass 検索結果をマッピングするBeanクラスオブジェクト
     * @param sqlId SQL_ID
     * @return 取得したBeanのリスト(該当0件の場合は空リスト)
     */
    public static <T> EntityList<T> findAllBySqlFile(final Class<T> entityClass, final String sqlId) {
        return daoContext().findAllBySqlFile(entityClass, sqlId, EMPTY_PARAM);
    }

    /**
     * SQL_IDをもとにバインド変数を展開して検索し、結果を格納したBeanを一件取得する。
     * <pre>
     * {@code
     * // 検索条件を引き渡すためのBeanを設定する
     * // FIND_BY_IDにBookエンティティのIDカラムがバインド変数として記述されている場合を想定
     * Book condition = new Book();
     * condition.setId(1L);
     *
     * Book book = UniversalDao.findBySqlFile(Book.class, "FIND_BY_ID", condition);
     * }</pre>
     * <p/>
     * 検索条件に該当するレコードが複数存在する場合、例外の送出は行わず、検索結果の先頭行を取得して返却する。
     * 確実に一行のレコードを取得する検索条件を設定すること。
     * <p/>
     *
     * @param <T> 検索結果をマッピングするBeanクラス
     * @param entityClass 検索結果をマッピングするBeanクラスオブジェクト
     * @param sqlId SQL_ID
     * @param params バインド変数
     * @return 1件のBean
     * @throws NoDataException (検索条件に該当するレコードが存在しない場合)
     */
    public static <T> T findBySqlFile(final Class<T> entityClass, final String sqlId, final Object params) {
        return daoContext().findBySqlFile(entityClass, sqlId, params);
    }

    /**
     * SQL_IDをもとに検索し、件数を取得する。
     * <p/>
     * 検索の詳細は{@link #countBySqlFile(Class, String, Object)}を参照すること。
     *
     * @param entityClass エンティティクラス
     * @param sqlId SQL_ID
     * @param <T> エンティティクラス
     * @return 件数
     */
    public static <T> long countBySqlFile(final Class<T> entityClass, final String sqlId) {
        return countBySqlFile(entityClass, sqlId, EMPTY_PARAM);
    }

    /**
     * SQL_IDをもとにバインド変数を展開して検索し、件数を取得する。
     * <p/>
     * 検索用のSQLを件数取得用のSQLへと変換して実行されるため、個別に件数取得用のSQLを作成する必要はない。
     *
     * @param entityClass エンティティクラス
     * @param sqlId SQL_ID
     * @param params バインド変数
     * @param <T> エンティティクラス
     * @return 件数
     */
    public static <T> long countBySqlFile(final Class<T> entityClass, final String sqlId, final Object params) {
        return daoContext().countBySqlFile(entityClass, sqlId, params);
    }

    /**
     * SQL_IDをもとに検索し、データが存在するか否かを確認する。
     * <p/>
     * 検索の詳細は{@link #exists(Class, String, Object)}を参照すること。
     * <p/>
     *
     * @param entityClass エンティティクラス
     * @param sqlId SQL_ID
     * @param <T> エンティティ型
     * @return 存在すればtrue
     */
    public static <T> boolean exists(final Class<T> entityClass, final String sqlId) {
        return exists(entityClass, sqlId, EMPTY_PARAM);
    }

    /**
     * SQL_IDをもとにバインド変数を展開して検索し、データが存在するか否かを確認する。
     * <p/>
     * 検索用のSQLを変換して使用する。
     * <p/>
     *
     * @param entityClass エンティティクラス
     * @param sqlId SQL_ID
     * @param params バインド変数
     * @param <T> エンティティ型
     * @return 存在すればtrue
     */
    public static <T> boolean exists(final Class<T> entityClass, final String sqlId, final Object params) {
        return countBySqlFile(entityClass, sqlId, params) > 0;
    }

    /**
     * 与えられたエンティティオブジェクトからアップデート文を生成し実行する。
     * <p/>
     * エンティティオブジェクトにてnullであるプロパティに対応するカラムは、そのままnullで更新される。
     * <p/>
     * 更新対象のエンティティに{@link jakarta.persistence.Version}が付与されたプロパティが存在する場合には、
     * 対象レコードは排他制御の対象となり、更新処理実行時に自動で排他制御が実行される。
     * <p/>
     * 排他制御の対象であるエンティティを更新する際は、以下の場合に{@link jakarta.persistence.OptimisticLockException}を送出する。
     * <ul>
     * <li>バージョン番号の不一致で、更新対象が存在しない場合</li>
     * <li>更新条件に合致する更新対象が存在しない場合</li>
     * </ul>
     * <p/>
     *
     * @param <T> エンティティクラス
     * @param entity エンティティオブジェクト
     * @return 更新件数
     * @throws jakarta.persistence.OptimisticLockException 更新対象が存在しない場合
     */
    public static <T> int update(final T entity) {
        return daoContext().update(entity);
    }

    /**
     * 与えられたエンティティ情報からアップデート文を生成し一括実行する。
     * <p/>
     * バージョン番号を用いた排他制御処理は行わない。
     * 排他制御を必要とする場合には、{@link #update(Object)}を使用すること。
     * もし、更新時にバージョン番号が不一致のエンティティオブジェクトが存在した場合、
     * そのレコードは更新されずに処理が正常に終了する。
     *
     * @param entities エンティティオブジェクトリスト
     * @param <T> エンティティクラス
     */
    public static <T> void batchUpdate(final List<T> entities) {
        daoContext().batchUpdate(entities);
    }

    /**
     * 与えられたエンティティオブジェクトからインサート文を生成し実行する。
     * <p/>
     * エンティティオブジェクトにてnullであるプロパティに対応するカラムは、そのままnullで登録される。
     * <p/>
     * {@link jakarta.persistence.GeneratedValue}が付与されているプロパティは採番された値が登録される。
     * <p/>
     * {@link jakarta.persistence.Version}が付与されたversionカラムに対して明示的に値を設定していたとしても、
     * 「0」で上書きされてinsertされる。
     *
     * @param <T> エンティティクラス
     * @param entity エンティティオブジェクト
     */
    public static <T> void insert(final T entity) {
        daoContext().insert(entity);
    }

    /**
     * 与えられたエンティティリストオブジェクトからインサート文を生成し一括実行する。
     * <p/>
     * エンティティオブジェクトにてnullであるプロパティに対応するカラムは、そのままnullで登録される。
     * <p/>
     * {@link jakarta.persistence.GeneratedValue}が付与されているプロパティは採番された値が登録される。
     * <p/>
     * {@link jakarta.persistence.Version}が付与されたversionカラムに対して明示的に値を設定していたとしても、
     * 「0」で上書きされてinsertされる。
     *
     * @param <T> エンティティクラス
     * @param entities エンティティリスト
     */
    public static <T> void batchInsert(final List<T> entities) {
        daoContext().batchInsert(entities);
    }

    /**
     * 与えられたエンティティオブジェクトからデリート文を生成し実行する。
     * <p/>
     * エンティティの主キーが削除条件となるため、主キー値以外のフィールドの値の有無は動作に影響しない。
     * <p/>
     *
     * @param <T> エンティティクラス
     * @param entity エンティティオブジェクト
     * @return 削除件数
     */
    public static <T> int delete(final T entity) {
        return daoContext().delete(entity);
    }

    /**
     * 与えられたエンティティオブジェクトからデリート文を生成し一括実行する。
     * <p/>
     * エンティティの主キーが削除条件となるため、主キー値以外のフィールドの値の有無は動作に影響しない。
     * <p/>
     *
     * @param entities エンティティリスト
     * @param <T> エンティティクラス
     */
    public static <T> void batchDelete(final List<T> entities) {
        daoContext().batchDelete(entities);
    }

    /**
     * ページ数を指定する。
     * <pre>
     * {@code
     *  // pageメソッドに「1」が与えられている場合に返却される件数は以下のようになる。
     *  // perメソッドに「10」を与える→1～10件目を返す
     *  // perメソッドに「20」を与える→1～20件目を返す
     * EntityList<Book> books = UniversalDao.page(1)
     *                   .per(20)
     *                   .findAllBySqlFile(Book.class, "FIND_ALL");
     *
     *  // pageメソッドに「2」が与えられている場合に返却される件数は以下のようになる。
     *  // perメソッドに「10」を与える→11～20件目を返す
     *  // perメソッドに「20」を与える→21～40件目を返す
     * EntityList<Book> books = UniversalDao.page(2)
     *                   .per(20)
     *                   .findAllBySqlFile(Book.class, "FIND_ALL");
     * }</pre>
     * <p/>
     * 表示対象のページ数におけるレコード件数が、perメソッドで与えたページ区切りに満たない場合は、取得可能な件数分を返却する。
     * <ul>
     * <li>perメソッドに10を与えていて、総件数が5件である場合、pageメソッドに1を与えた場合は1～5件目を返却する。</li>
     * <li>perメソッドに10を与えていて、総件数が15件である場合、pageメソッドに2を与えた場合は11～15件目を返却する。</li>
     * </ul>
     * <p/>
     * ページ変更の度に検索処理を行うことになるため、ソートを使用して検索結果の出力順を固定すること。
     *
     * @param  page ページ数
     * @return DaoContext
     */
    public static DaoContext page(final long page) {
        return daoContext().page(page);
    }

    /**
     * 1ページにつき何件取得するかを指定する。
     * <pre>
     * {@code
     *  // perメソッドに「10」が与えられている場合は、返却される件数は以下のようになる。
     *  // pageメソッドに「1」を与える→1～10件目を返す
     *  // pageメソッドに「2」を与える→11～20件目を返す
     * EntityList<Book> books = UniversalDao.page(1)
     *                   .per(10)
     *                   .findAllBySqlFile(Book.class, "FIND_ALL");
     *
     *  // perメソッドに「20」が与えられている場合は、返却される件数は以下のようになる。
     *  // pageメソッドに「1」を与える→1～20件目を返す
     *  // pageメソッドに「2」を与える→21～40件目を返す
     * EntityList<Book> books = UniversalDao.page(1)
     *                   .per(20)
     *                   .findAllBySqlFile(Book.class, "FIND_ALL");
     * }</pre>
     * <p/>
     * 表示対象のページ数におけるレコード件数が、perメソッドで与えたページ区切りに満たない場合は、取得可能な件数分を返却する。
     * perメソッドに10を与えていて、総件数が5件である場合、pageメソッドに1を与えた場合は1～5件目を返却する。
     * perメソッドに10を与えていて、総件数が15件である場合、pageメソッドに2を与えた場合は11～15件目を返却する。
     * <p/>
     *
     * @param per 取得する件数
     * @return DaoContext
     */
    public static DaoContext per(final long per) {
        return daoContext().per(per);
    }

    /**
     * 検索結果の取得を遅延させる。
     * <p/>
     * 大量データを検索する場合でもヒープを圧迫することなく安全に検索結果を扱うことができる。
     * <pre>
     * {@code
     * // サーバサイドカーソルを利用するためclose処理を行う必要がある
     * try (DeferredEntityList<Project> searchList =  (DeferredEntityList<Project>) UniversalDao
     *         .defer()
     *         .findAllBySqlFile(Project.class, "SEARCH_PROJECT",searchCondition)) {
     *     for (Project project : searchList) {
     *         // projectを利用した処理
     *     }
     * }
     * }</pre>
     * <p/>
     *
     * @return DaoContext
     */
    public static DaoContext defer() {
        return daoContext().defer();
    }

    /**
     * トランザクション境界を作るためのクラス。
     * <p/>
     * 通常の業務トランザクションと異なるトランザクションでデータベースアクセスを行いたい場合、
     * 本クラスを継承することで別トランザクション内で{@link nablarch.common.dao.UniversalDao}を使用することができる。
     * <p/>
     * <pre>
     * {@code
     * // projectエンティティを登録する場合
     * final Project project = SessionUtil.get(context, "project");
     *
     * new UniversalDao.Transaction("トランザクションマネージャ名"){
     *
     *     // execute()を実装する
     *     protected void execute() {
     *        // UniversalDaoを利用したDB操作処理を記述する
     *        UniversalDao.insert(project);
     *     }
     * };
     * }</pre>
     *
     * @author kawasima
     * @author Hisaaki Shioiri
     */
    public abstract static class Transaction extends SimpleDbTransactionExecutor<Void> {

        /**
         * トランザクションマネージャを指定して、別トランザクションを生成する。
         *
         * @param transactionManager トランザクションマネージャ
         */
        public Transaction(final SimpleDbTransactionManager transactionManager) {
            super(transactionManager);
            doTransaction();
        }

        /**
         * トランザクションマネージャ名を指定して、別トランザクションを生成する。
         *
         * @param transactionManagerName トランザクションマネージャ名
         */
        public Transaction(final String transactionManagerName) {
            this((SimpleDbTransactionManager) SystemRepository.get(transactionManagerName));
        }

        @Override
        public Void execute(final AppDbConnection connection) {
            final DaoContextFactory daoContextFactory = SystemRepository.get(DAO_CONTEXT_FACTORY);
            if (daoContextFactory == null) {
                throw new IllegalStateException(
                        "Transaction feature requires DaoContextFactory is registered to the SystemRepository.");
            }
            final AppDbConnection origConn = daoContextFactory.getDbConnection();
            daoContextFactory.setDbConnection(connection);

            try {
                execute();
                return null;
            } finally {
                daoContextFactory.setDbConnection(origConn);
            }
        }

        /**
         * データベースへのアクセス処理を行う。
         * <p/>
         * コネクションはDaoContextFactoryに設定されたものが使われるので、
         * このexecuteを実装し、中でUniversalDaoのメソッドを使えば 別トランザクションになる。
         * <p/>
         * また、自動的にコミット/ロールバックが行われる。
         *
         */
        protected abstract void execute();
    }
}

