package nablarch.common.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * パラメータとSQLを格納する内部クラス。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
public class SqlWithParams implements Serializable {

    /** SQL文 */
    private final String sql;

    /** パラメータ */
    private final List<Object> params;

    /** バインド変数に対応したカラムのメタ情報 */
    private final List<ColumnMeta> columns;

    /**
     * コンストラクタ。
     *
     * @param sql SQL文
     * @param params SQLに埋め込むパラメータ
     * @param columns カラムのメタ情報
     */
    public SqlWithParams(final String sql, final List<Object> params, List<ColumnMeta> columns) {
        this.sql = sql;
        this.params = Collections.unmodifiableList(params);
        this.columns = columns;
    }

    /**
     * SQLを返す。
     *
     * @return SQL文
     */
    public String getSql() {
        return sql;
    }

    /**
     * パラメータリストを返す。
     *
     * @return パラメータのリスト
     */
    public List<Object> getParams() {
        return new ArrayList<Object>(params);
    }

    /**
     * バインド変数に対応したカラムのメタ情報のリストを返す。
     *
     * @return カラムのメタ情報のリスト
     */
    public List<ColumnMeta> getColumns() {
        return new ArrayList<ColumnMeta>(columns);
    }
}
