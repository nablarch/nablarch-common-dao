package nablarch.common.dao;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.repository.SystemRepository;

/**
 * データベースに関するユーティリティクラス。
 *
 * @author hisaaki sioiri
 */
public final class DatabaseUtil {

    /** データベースからメタ情報を取得するクラス */
    private static final DatabaseMetaDataExtractor DEFAULT_METADATA_EXTRACTOR = new DatabaseMetaDataExtractor();

    /** 隠蔽コンストラクタ。 */
    private DatabaseUtil() {
    }

    /**
     * 主キー情報を取得する。
     *
     * @param tableName テーブル名
     * @return 主キー情報(キー:カラム名、値:ポジション)
     */
    public static Map<String, Short> getPrimaryKey(String tableName) {
        return getDatabaseMetaDataExtractor().getPrimaryKeys(tableName);
    }

    /**
     * カラムのSQL型を取得する。
     * スキーマ名を指定しない場合、デフォルトスキーマが対象となる。
     *
     * @param schemaName スキーマ名
     * @param tableName テーブル名
     * @return カラムのSQL型(キー:カラム名、値:SQL型)
     */
    public static Map<String, Integer> getSqlTypeMap(String schemaName, String tableName) {
        return getDatabaseMetaDataExtractor().getSqlTypeMap(schemaName, tableName);
    }

    /**
     * 識別子をデータベースメタ情報の定義を元に変換する。
     *
     * @param identifiers 識別子
     * @return 変換後の値
     */
    public static String convertIdentifiers(String identifiers) {
        try {
            return doConvertIdentifiers(getMetaData(), identifiers);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 識別子をデータベースメタ情報の定義を元に変換する。
     *
     * @param metaData データベースメタ情報
     * @param identifiers 識別子
     * @return 変換後の値
     */
    public static String convertIdentifiers(DatabaseMetaData metaData, String identifiers) {
        try {
            return doConvertIdentifiers(metaData, identifiers);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 識別子をデータベースメタ情報の定義を元に変換する。
     *
     * @param metaData データベースメタ情報
     * @param identifiers 識別子
     * @return 変換後の値
     * @throws SQLException データベース関連の例外
     */
    public static String doConvertIdentifiers(DatabaseMetaData metaData, String identifiers) throws SQLException {
        if (metaData.storesMixedCaseIdentifiers()) {
            return identifiers;
        } else if (metaData.storesUpperCaseIdentifiers()) {
            return identifiers.toUpperCase();
        } else if (metaData.storesLowerCaseIdentifiers()) {
            return identifiers.toLowerCase();
        }
        return identifiers;
    }

    /**
     * データベースメタデータを取得する。
     *
     * @return データベースメタデータ
     * @throws SQLException データベース関連の例外
     */
    public static DatabaseMetaData getMetaData() throws SQLException {
        AppDbConnection connection = DbConnectionContext.getConnection();

        if (!(connection instanceof TransactionManagerConnection)) {
            throw new IllegalStateException("failed to get DatabaseMetaData.");
        }

        return ((TransactionManagerConnection) connection).getConnection().getMetaData();
    }

    /**
     * {@link DatabaseMetaDataExtractor}を取得する。
     * <p/>
     * {@link SystemRepository}上に設定がない場合は、デフォルト実装の{@link DatabaseMetaDataExtractor}を返す。
     *
     * @return {@link DatabaseMetaDataExtractor}
     */
    private static DatabaseMetaDataExtractor getDatabaseMetaDataExtractor() {
        final DatabaseMetaDataExtractor databaseMetaDataExtractor = SystemRepository.get("databaseMetaDataExtractor");
        return databaseMetaDataExtractor == null ? DEFAULT_METADATA_EXTRACTOR : databaseMetaDataExtractor;
    }
}

