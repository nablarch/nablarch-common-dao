package nablarch.common.dao;

import nablarch.common.idgenerator.IdGenerator;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.util.annotation.Published;

/**
 * {@link DaoContext}を生成するファクトリクラス。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public abstract class DaoContextFactory {

    /** シーケンスID採番用の{@link IdGenerator}実装クラス */
    protected IdGenerator sequenceIdGenerator;      // SUPPRESS CHECKSTYLE サブクラスで使用するフィールドのため。

    /** テーブル採番用の{@link IdGenerator}の実装クラス */
    protected IdGenerator tableIdGenerator;         // SUPPRESS CHECKSTYLE サブクラスで使用するフィールドのため。

    /** スレッド上に保持するデータベース接続 */
    protected ThreadLocal<AppDbConnection> dbConnection = new ThreadLocal<AppDbConnection>();       // SUPPRESS CHECKSTYLE サブクラスで使用するフィールドのため。

    /** SQLビルダー({@link nablarch.common.dao.StandardSqlBuilder}) */
    protected StandardSqlBuilder sqlBuilder = new StandardSqlBuilder();       // SUPPRESS CHECKSTYLE サブクラスで使用するフィールドのため。

    /**
     * DaoContextを生成する。
     *
     * @return DaoContext
     */
    public abstract DaoContext create();

    /**
     * シーケンスIDジェネレータを設定する。
     *
     * @param sequenceIdGenerator シーケンスIDジェネレータ
     */
    public void setSequenceIdGenerator(final IdGenerator sequenceIdGenerator) {
        this.sequenceIdGenerator = sequenceIdGenerator;
    }

    /**
     * テーブルIDジェネレータを設定する。
     *
     * @param tableIdGenerator テーブルIDジェネレータ
     */
    public void setTableIdGenerator(final IdGenerator tableIdGenerator) {
        this.tableIdGenerator = tableIdGenerator;
    }

    /**
     * SQLを構築するビルダー({@link nablarch.common.dao.StandardSqlBuilder})を設定する。
     * <p/>
     * 設定しない場合は、{@link nablarch.common.dao.StandardSqlBuilder}が使用される。
     *
     * @param sqlBuilder SQLビルダー
     */
    public void setSqlBuilder(StandardSqlBuilder sqlBuilder) {
        this.sqlBuilder = sqlBuilder;
    }

    /**
     * DAOで使うコネクションを設定する。
     * (トランザクション用です)
     *
     * @param aConn データベースコネクション
     */
    public void setDbConnection(final AppDbConnection aConn) {
        if (aConn == null) {
            dbConnection.remove();
        } else {
            this.dbConnection.set(aConn);
        }
    }

    /**
     * DAOで使うコネクションを取得する。
     * (トランザクション用です)
     *
     * @return データベースコネクション
     */
    public AppDbConnection getDbConnection() {
        return dbConnection.get();
    }
}

