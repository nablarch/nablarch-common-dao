package nablarch.common.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 一括実行用(execute batch用)のSQL文とバインド変数に応じたカラムリストを保持するクラス。
 *
 * @author Hisaaki Shioiri
 */
public class BatchSqlWithColumns {

    /** SQL文 */
    private final String sql;

    /** バインド変数に対応したカラムのリスト */
    private final List<ColumnMeta> columns;

    /**
     * SQL文とカラムリストを保持する{@code BatchSqlWithColumns}を生成する。
     *
     * @param sql SQL文
     * @param columns カラムリスト
     */
    public BatchSqlWithColumns(final String sql, final List<ColumnMeta> columns) {
        this.sql = sql;
        this.columns = Collections.unmodifiableList(columns);
    }

    /**
     * SQL文を返す。
     *
     * @return SQL文
     */
    public String getSql() {
        return sql;
    }

    /**
     * バインド変数に対応したカラムリストを返す。
     *
     * @return バインド変数に対応したカラムリスト
     */
    public List<ColumnMeta> getColumns() {
        return new ArrayList<ColumnMeta>(columns);
    }
}
