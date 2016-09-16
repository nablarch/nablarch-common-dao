package nablarch.common.dao;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import nablarch.core.util.annotation.Published;

/**
 * JDBCの{@link DatabaseMetaData}からメタ情報を取得するクラス。
 * <p/>
 * JDBCの{@link DatabaseMetaData}から情報を取得できないデータベース構成の場合には、
 * 本クラスを継承し実装を差し替えること。
 *
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class DatabaseMetaDataExtractor {

    /**
     * 主キー情報を{@link DatabaseMetaData}から取得する。
     * @param tableName テーブル名
     * @return 主キー情報(key: カラム名, value: カラムポジション)
     */
    public Map<String, Short> getPrimaryKeys(String tableName) {
        try {
            final DatabaseMetaData metaData = DatabaseUtil.getMetaData();
            return toPrimaryKeyMap(
                    metaData.getPrimaryKeys(null, null, DatabaseUtil.doConvertIdentifiers(metaData, tableName)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 主キー情報をキーがカラム名、値がカラムポジションのMapに変換する。
     *
     * @param resultSet 主キー情報({@link DatabaseMetaData#getPrimaryKeys(String, String, String)}の結果
     * @return 変換した値
     * @throws SQLException データベース例外
     */
    private static Map<String, Short> toPrimaryKeyMap(ResultSet resultSet) throws SQLException {
        try {
            final Map<String, Short> result = new HashMap<String, Short>();
            while (resultSet.next()) {
                result.put(
                        resultSet.getString("COLUMN_NAME")
                                .toUpperCase(),
                        resultSet.getShort("KEY_SEQ")
                );
            }
            return result;
        } finally {
            resultSet.close();
        }
    }
}
