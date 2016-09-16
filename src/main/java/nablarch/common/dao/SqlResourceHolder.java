package nablarch.common.dao;

import nablarch.core.db.statement.ResultSetIterator;

/**
 * SQLの検索結果リソースを保持するクラス。
 *
 * @author kawashima
 * @author Hisaaki Shioiri
 */
class SqlResourceHolder {

    /** SQLの検索結果 */
    private final ResultSetIterator resultSetIterator;

    /**
     * コンストラクタ。
     *
     * @param resultSetIterator 検索結果
     */
    SqlResourceHolder(ResultSetIterator resultSetIterator) {
        this.resultSetIterator = resultSetIterator;
    }

    /**
     * {@link nablarch.core.db.statement.ResultSetIterator}を取得する。
     *
     * @return リザルトセットイテレータ
     */
    public ResultSetIterator getResultSetIterator() {
        return resultSetIterator;
    }

    /**
     * SQLリソースを解放する。
     */
    public void dispose() {
        resultSetIterator.close();
    }
}

