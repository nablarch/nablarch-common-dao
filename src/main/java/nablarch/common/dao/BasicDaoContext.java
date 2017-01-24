package nablarch.common.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.Entity;
import javax.persistence.GenerationType;
import javax.persistence.OptimisticLockException;

import nablarch.common.idgenerator.IdGenerator;
import nablarch.core.db.DbAccessException;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.ResultSetIterator;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlRow;

/**
 * {@link nablarch.common.dao.DaoContext}のデフォルト実装クラス。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
public class BasicDaoContext implements DaoContext {

    /** デフォルトのページサイズ（取得件数） */
    private static final long DEFAULT_PER = 25L;

    /** サイズ0のオブジェクト配列 */
    private static final Object[] EMPTY_PARAMS = new Object[0];

    /** データベース接続 */
    private AppDbConnection dbConnection;

    /** ページ番号 */
    private Long page;

    /** ページサイズ（取得件数） */
    private Long per;

    /** 遅延ロードするか否か */
    private boolean defer = false;

    /** {@link GenerationType}と{@link IdGenerator}との対応表 */
    private final Map<GenerationType, IdGenerator> idGenerators =
            new EnumMap<GenerationType, IdGenerator>(GenerationType.class);

    /** SQLビルダー */
    private final StandardSqlBuilder sqlBuilder;

    /** データベース方言 */
    private final Dialect dialect;

    /**
     * 実行コンテキストを生成する。
     * <p/>
     * 本クラスは直接インスタンス化するのではなく、ファクトリクラス({@link nablarch.common.dao.BasicDaoContextFactory}からインスタンス化する。
     * @param sqlBuilder SQLビルダー
     * @param dialect データベース方言
     */
    BasicDaoContext(StandardSqlBuilder sqlBuilder, Dialect dialect) {
        this.sqlBuilder = sqlBuilder;
        this.dialect = dialect;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * この実装では、プライマリーキーのメタデータを{@link java.sql.DatabaseMetaData}から取得する。
     * それに失敗した場合にこのメソッドを呼ぶと、{@link IllegalStateException}を送出する。
     */
    @Override
    public <T> T findById(final Class<T> entityClass, final Object... id) {
        final List<ColumnMeta> idColumns = EntityUtil.findIdColumns(entityClass);
        if (id.length != idColumns.size()) {
            throw new IllegalArgumentException("Mismatch the counts of id columns. expected=" + idColumns.size());
        }
        final String sql = sqlBuilder.buildSelectByIdSql(entityClass);
        final SqlPStatement stmt = dbConnection.prepareStatement(sql);
        for (int i = 0; i < idColumns.size(); i++) {
            final ColumnMeta meta = idColumns.get(i);
            if (meta.getSqlType() == null) {
                throw new IllegalStateException("Unable to get SQL type from DB.");
            }
            stmt.setObject(i + 1, id[i], meta.getSqlType());
        }
        final ResultSetIterator rsIter = stmt.executeQuery();
        if (!rsIter.next()) {
            throw new NoDataException();
        }

        final SqlRow row = rsIter.getRow();
        return EntityUtil.createEntity(entityClass, row);
    }

    @Override
    public <T> EntityList<T> findAll(final Class<T> entityClass) {
        final String sql = sqlBuilder.buildSelectAllSql(entityClass);
        final SqlPStatement stmt = dbConnection.prepareStatement(sql);
        final SqlResourceHolder holder = new SqlResourceHolder(stmt.executeQuery());

        if (defer) {
            return new DeferredEntityList<T>(entityClass, holder);
        } else {
            final EntityList<T> results = new EntityList<T>();
            ResultSetIterator rows = holder.getResultSetIterator();
            for (SqlRow row : rows) {
                results.add(EntityUtil.createEntity(entityClass, row));
            }
            return results;
        }
    }

    @Override
    public <T> EntityList<T> findAllBySqlFile(final Class<T> entityClass, final String sqlId, final Object params) {
        if (page == null) {
            return findAllBySqlFileWithoutPaginate(entityClass, sqlId, params);
        } else {
            return findAllBySqlFIleWithPaginate(entityClass, sqlId, params);
        }
    }

    @Override
    public <T> EntityList<T> findAllBySqlFile(final Class<T> entityClass, final String sqlId) {
        return findAllBySqlFile(entityClass, sqlId, EMPTY_PARAMS);
    }

    /**
     * 検索クエリを実行する。
     *
     * @param normalizedSqlId SQL ID
     * @param params バインド変数
     * @param selectOption 検索オプション
     * @return 検索結果
     */
    @SuppressWarnings("unchecked")
    protected SqlResourceHolder executeQuery(final String normalizedSqlId, final Object params, SelectOption selectOption) {
        if (params.getClass().isArray()) {
            final Object[] paramsArray = (Object[]) params;
            final SqlPStatement stmt = dbConnection
                    .prepareStatementBySqlId(normalizedSqlId, selectOption);
            for (int i = 0; i < paramsArray.length; i++) {
                stmt.setObject(i + 1, paramsArray[i]);
            }
            return new SqlResourceHolder(stmt.executeQuery());
        } else {
            final ParameterizedSqlPStatement stmt = dbConnection
                    .prepareParameterizedSqlStatementBySqlId(normalizedSqlId, params, selectOption);
            if (params instanceof Map) {
                return new SqlResourceHolder(stmt.executeQueryByMap((Map<String, ?>) params));
            } else {
                return new SqlResourceHolder(stmt.executeQueryByObject(params));
            }
        }
    }

    /**
     * ページングなしの場合の検索を実行する。
     *
     * @param entityClass エンティティクラス
     * @param sqlId SQL ID
     * @param params バインド変数
     * @param <T> エンティティクラス
     * @return エンティティクラスのリスト
     */
    protected <T> EntityList<T> findAllBySqlFileWithoutPaginate(
            final Class<T> entityClass, final String sqlId, final Object params) {

        final SqlResourceHolder holder = executeQuery(normalizeSqlId(sqlId, entityClass), params, new SelectOption(0, 0));
        if (defer) {
            return new DeferredEntityList<T>(entityClass, holder);
        } else {
            final EntityList<T> results = new EntityList<T>();
            ResultSetIterator rows = holder.getResultSetIterator();
            for (SqlRow row : rows) {
                results.add(createResultInstance(entityClass, row));
            }
            results.setResultCount(results.size());
            return results;
        }
    }

    /**
     * 検索結果オブジェクトを生成する。
     *
     * @param entityClass エンティティクラス。
     * @param row 検索結果
     * @param <T> 総称型
     * @return 検索結果オブジェクト
     */
    private <T> T createResultInstance(final Class<T> entityClass, final SqlRow row) {
        if (entityClass.equals(SqlRow.class)) {
            @SuppressWarnings("unchecked")
            final T t = (T) row;
            return t;
        } else {
            return EntityUtil.createEntity(entityClass, row);
        }
    }

    /**
     * ページネーションつきの検索を実行する。
     * <p/>
     * 遅延ロード({@link #defer}がtrueの場合)、{@link IllegalArgumentException}を送出する。
     *
     * @param entityClass エンティティクラス
     * @param sqlId SQL ID
     * @param params バインド変数
     * @param <T> エンティティクラス
     * @return エンティティリストを返します。
     */
    protected <T> EntityList<T> findAllBySqlFIleWithPaginate(
            final Class<T> entityClass, final String sqlId, final Object params) {
        if (defer) {
            throw new IllegalArgumentException("Can't search with defer and pagination.");
        }
        final long count = countBySqlFile(entityClass, sqlId, params);
        final EntityList<T> results = new EntityList<T>();
        results.setPage(page);
        results.setMax(per);
        results.setResultCount(count);

        final SqlResourceHolder holder = executeQuery(normalizeSqlId(sqlId, entityClass), params,
                new SelectOption(results.getPagination().getStartPosition(), results.getPagination().getMax()));
        try {
            for (SqlRow row : holder.getResultSetIterator()) {
                results.add(createResultInstance(entityClass, row));
            }
        } finally {
            holder.dispose();
        }
        return results;
    }

    /**
     * SQL_IDをもとに検索処理を行いEntityを取得する。
     *
     * @param <T> 総称型
     * @param entityClass エンティティクラス
     * @param sqlId SQL_ID
     * @param params バインド変数
     * @return 1件のEntity。見つからない場合はNoDataExceptionを送出する。
     */
    @Override
    public <T> T findBySqlFile(final Class<T> entityClass, final String sqlId, final Object params) {
        final SqlResourceHolder holder = executeQuery(normalizeSqlId(sqlId, entityClass), params, new SelectOption(0, 0));
        try {
            ResultSetIterator rows = holder.getResultSetIterator();
            if (rows.next()) {
                final SqlRow row = holder.getResultSetIterator().getRow();
                if (entityClass.equals(SqlRow.class)) {
                    @SuppressWarnings("unchecked")
                    T t = (T) row;
                    return t;
                } else {
                    return EntityUtil.createEntity(entityClass, row);
                }
            } else {
                throw new NoDataException();
            }
        } finally {
            holder.dispose();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> long countBySqlFile(final Class<T> entityClass, final String sqlId, final Object params) {

        final ResultSetIterator rs;
        if (params.getClass().isArray()) {
            final Object[] paramsArray = (Object[]) params;
            final SqlPStatement stmtCount = dbConnection.prepareCountStatementBySqlId(
                    normalizeSqlId(sqlId, entityClass));
            for (int i = 0; i < paramsArray.length; i++) {
                stmtCount.setObject(i + 1, paramsArray[i]);
            }

            rs = stmtCount.executeQuery();
        } else {
            final ParameterizedSqlPStatement stmtCount = dbConnection
                    .prepareParameterizedCountSqlStatementBySqlId(
                            normalizeSqlId(sqlId, entityClass), params);

            if (params instanceof Map) {
                rs = stmtCount.executeQueryByMap((Map<String, ?>) params);
            } else {
                rs = stmtCount.executeQueryByObject(params);
            }
        }
        return getCountQueryResult(rs);
    }

    /**
     * 件数取得クエリから結果を取得する。
     *
     * @param rs 件数取得クエリの実行結果
     * @return 件数
     */
    private long getCountQueryResult(ResultSetIterator rs) {
        long count;
        try {
            if (rs.next()) {
                count =  rs.getLong(1);
            } else {
                throw new IllegalStateException("Count query didn't return result.");
            }
        } finally {
            rs.close();
        }
        return count;
    }

    @Override
    public <T> int update(final T entity) throws OptimisticLockException {
        final SqlWithParams sqlWithParams = sqlBuilder.buildUpdateSql(entity);

        final SqlPStatement stmt = dbConnection.prepareStatement(sqlWithParams.getSql());

        setObjects(stmt, sqlWithParams);
        final int rows = stmt.executeUpdate();
        if ((EntityUtil.findVersionColumn(entity) != null) && (rows == 0)) {
            throw new OptimisticLockException();
        }
        return rows;
    }

    @Override
    public <T> void batchUpdate(final List<T> entities) {
        if (entities.isEmpty()) {
            return;
        }

        final Class<?> entityClass = entities.get(0).getClass();
        final BatchSqlWithColumns sqlWithColumns = sqlBuilder.buildBatchUpdateSql(entityClass);

        final SqlPStatement stmt = dbConnection.prepareStatement(sqlWithColumns.getSql());

        final List<ColumnMeta> columns = sqlWithColumns.getColumns();
        for (T entity : entities) {
            addBatchParameter(stmt, entity, columns);
        }
        stmt.executeBatch();
    }

    @Override
    public <T> void insert(final T entity) {

        ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(entity);
        GenerationType generationType = findGeneratedType(generatedValueColumn);
        preInsert(entity, generationType);

        final SqlWithParams sqlWithParams;
        final SqlPStatement stmt;
        if (generationType == GenerationType.IDENTITY) {
            sqlWithParams = sqlBuilder.buildInsertWithIdentityColumnSql(entity);
            stmt = dbConnection.prepareStatement(sqlWithParams.getSql(), new String[]{
                    DatabaseUtil.convertIdentifiers(generatedValueColumn.getName())});
        } else {
            sqlWithParams = sqlBuilder.buildInsertSql(entity);
            stmt = dbConnection.prepareStatement(sqlWithParams.getSql());
        }

        setObjects(stmt, sqlWithParams);
        stmt.executeUpdate();

        postInsert(entity, generationType, stmt);
    }

    /**
     * データベースの定義情報に基づいて型変換したパラメータをステートメントに設定する。
     * @param stmt ステートメント
     * @param sqlWithParams パラメータ
     */
    private void setObjects(SqlPStatement stmt, SqlWithParams sqlWithParams) {
        for (int i = 0; i < sqlWithParams.getParamSize(); i++) {
            if (sqlWithParams.getColumn(i).getSqlType() == null) {
                throw new IllegalStateException("Unable to get SQL type from DB.");
            }
            stmt.setObject(i + 1, sqlWithParams.getParam(i),
                    sqlWithParams.getColumn(i).getSqlType());
        }
    }

    @Override
    public <T> void batchInsert(final List<T> entities) {
        if (entities.isEmpty()) {
            return;
        }

        @SuppressWarnings("unchecked")
        final Class<T> entityClass = (Class<T>) entities.get(0).getClass();

        final ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(entityClass);
        final GenerationType generationType = findGeneratedType(generatedValueColumn);

        final BatchSqlWithColumns sqlWithColumns;
        final SqlPStatement stmt;
        if (generationType == GenerationType.IDENTITY) {
            sqlWithColumns = sqlBuilder.buildBatchInsertWithIdentityColumnSql(entityClass);
            stmt = dbConnection.prepareStatement(sqlWithColumns.getSql(),
                    new String[] {DatabaseUtil.convertIdentifiers(generatedValueColumn.getName())});
        } else {
            sqlWithColumns = sqlBuilder.buildBatchInsertSql(entityClass);
            stmt = dbConnection.prepareStatement(sqlWithColumns.getSql());
        }
        final List<ColumnMeta> columns = sqlWithColumns.getColumns();

        for (T entity : entities) {
            preInsert(entity, generationType);
            addBatchParameter(stmt, entity, columns);
        }
        stmt.executeBatch();

        postBatchInsert(entityClass, entities, generationType, stmt);
    }

    /**
     * このEntityの採番タイプを取得する。
     *
     * @param generatedValueColumn 採番対象カラム
     * @return 採番タイプ(採番カラムが存在しない場合はnull)
     */
    private GenerationType findGeneratedType(final ColumnMeta generatedValueColumn) {
        if (generatedValueColumn == null) {
            return null;
        }
        final GenerationType type = generatedValueColumn.getGenerationType();
        final GenerationType result;
        if (type == GenerationType.AUTO) {
            result = getAutoType();
        } else if (type == GenerationType.SEQUENCE) {
            verifySequenceGenerator();
            result = type;
        } else if (type == GenerationType.IDENTITY) {
            verifyIdentityGenerator();
            result = type;
        } else {
            result = type;
        }
        return result;
    }

    /**
     * IDENTITY採番(データベース側の自動採番カラムを用いた採番)が利用可能か検証する。
     *
     * IDENTITY採番が利用不可な場合には、例外を送出する。
     *
     */
    private void verifyIdentityGenerator() {
        if (!dialect.supportsIdentity()) {
            throw new IllegalEntityException(
                    MessageFormat.format(
                            "Unsupported GenerationType in dialect. GenerationType = {0}, Dialect class = {1}",
                            GenerationType.IDENTITY, dialect.getClass().getName()));
        }
    }

    /**
     * シーケンス採番が利用可能か検証する。
     *
     * シーケンス採番が利用不可な場合には、例外を送出する。
     *
     */
    private void verifySequenceGenerator() {
        if (!dialect.supportsSequence()) {
            throw new IllegalEntityException(
                    MessageFormat.format(
                            "Unsupported GenerationType in dialect. GenerationType = {0}, Dialect class = {1}",
                            GenerationType.SEQUENCE, dialect.getClass().getName()));
        }
    }

    /**
     * ダイアレクトを元に採番サイプを取得する。
     * @return 採番タイプ
     */
    private GenerationType getAutoType() {
        if (dialect.supportsIdentity()) {
            return GenerationType.IDENTITY;
        } else if (dialect.supportsSequence()) {
            return GenerationType.SEQUENCE;
        } else {
            return GenerationType.TABLE;
        }
    }

    /**
     * INSERT実行前の事前処理を行う。
     *
     * 以下の事前処理を行う。
     * <ul>
     *     <li>バージョン番号へ初期バージョン(0)の設定</li>
     *     <li>シーケンスまたはテーブル採番カラムへ採番した値の設定</li>
     * </ul>
     *
     * @param <T> 総称型
     * @param entity エンティティ
     * @param generationType 採番タイプ
     */
    private <T> void preInsert(T entity, GenerationType generationType) {
        final ColumnMeta versionColumn = EntityUtil.findVersionColumn(entity);
        if (versionColumn != null
                && Number.class.isAssignableFrom(versionColumn.getPropertyType())) {
            EntityUtil.setProperty(entity, versionColumn.getPropertyName(),
                    convertToPropertyType(0L, versionColumn));
        }

        if (generationType == null || generationType == GenerationType.IDENTITY) {
            return;
        }

        ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(entity);
        IdGenerator generator = idGenerators.get(generationType);
        String id = generator.generateId(generatedValueColumn.getGeneratorName());
        EntityUtil.setProperty(entity, generatedValueColumn.getPropertyName(),
                convertToPropertyType(id, generatedValueColumn));
    }

    /**
     * INSERT実行後の事後処理を行う。
     *
     * 以下の処理を行う。
     * <ul>
     *     <li>IDENTITY採番のカラムへDB側で採番した値の設定</li>
     * </ul>
     *
     * @param <T> 総称型
     * @param entity エンティティ
     * @param generationType 採番タイプ
     * @param statement INSERT処理を実行したステートメント
     */
    private <T> void postInsert(
            T entity, GenerationType generationType, SqlPStatement statement) {
        if (generationType != GenerationType.IDENTITY) {
            return;
        }
        final ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(entity);
        ResultSet keys = statement.getGeneratedKeys();
        try {
            if (keys.next()) {
                String id = keys.getString(1);
                EntityUtil.setProperty(entity, generatedValueColumn.getPropertyName(),
                        convertToPropertyType(id, generatedValueColumn));
            }
        } catch (SQLException e) {
            throw new DbAccessException("failed to get auto generated key. entity name = "
                    + entity.getClass().getName(), e);
        } finally {
            if (keys != null) {
                try {
                    keys.close();
                } catch (SQLException ignored) {        // SUPPRESS CHECKSTYLE 例外を抑止するため
                }
            }
        }
    }

    /**
     * 一括登録(batch insert)実行後の事後処理を行う。
     *
     * @param entityClass エンティティクラス
     * @param entities エンティティリスト
     * @param generationType 採番タイプ
     * @param statement 一括登録(batch INSERT)を実行したステートメント
     */
    private <T> void postBatchInsert(
            final Class<T> entityClass,
            final Collection<T> entities,
            final GenerationType generationType,
            final SqlPStatement statement) {

        if (generationType != GenerationType.IDENTITY) {
            return;
        }

        final ColumnMeta generatedValueColumn = EntityUtil.findGeneratedValueColumn(entityClass);
        final ResultSet keys = statement.getGeneratedKeys();
        try {
            for (T entity : entities) {
                if (keys.next()) {
                    String id = keys.getString(1);
                    EntityUtil.setProperty(entity, generatedValueColumn.getPropertyName(),
                            convertToPropertyType(id, generatedValueColumn));
                } else {
                    throw new IllegalStateException(
                            "generated key not found. entity name=[" + entityClass.getName() + ']');
                }
            }
        } catch (SQLException e) {
            throw new DbAccessException("failed to get auto generated key. entity name = "
                    + entityClass.getName(), e);
        } finally {
            if (keys != null) {
                try {
                    keys.close();
                } catch (SQLException ignored) {        // SUPPRESS CHECKSTYLE 例外を抑止するため
                }
            }
        }
    }


    @Override
    public <T> int delete(final T entity) {
        final SqlWithParams sqlWithParams = sqlBuilder.buildDeleteSql(entity);

        final SqlPStatement stmt = dbConnection.prepareStatement(sqlWithParams.getSql());
        final Iterator<Object> valueIter = sqlWithParams.getParams().iterator();

        int index = 1;
        while (valueIter.hasNext()) {
            stmt.setObject(index, valueIter.next());
            index++;
        }
        return stmt.executeUpdate();
    }

    @Override
    public <T> void batchDelete(final List<T> entities) {
        if (entities.isEmpty()) {
            return;
        }

        final Class<?> entityClass = entities.get(0).getClass();
        final BatchSqlWithColumns sqlWithColumns = sqlBuilder.buildBatchDeleteSql(entityClass);
        final SqlPStatement stmt = dbConnection.prepareStatement(sqlWithColumns.getSql());

        final List<ColumnMeta> columns = sqlWithColumns.getColumns();
        for (T entity : entities) {
            addBatchParameter(stmt, entity, columns);
        }
        stmt.executeBatch();
    }

    /**
     * 一括実行用にパラメータ設定と{@link SqlPStatement#addBatch()}を行う。
     *
     * @param statement 一括実行用のステートメント
     * @param entity 一括実行対象のエンティティ
     * @param columns パラメータのカラムリスト
     * @param <T> エンティティクラス
     */
    private static <T> void addBatchParameter(
            final SqlPStatement statement, final T entity, final List<ColumnMeta> columns) {
        final Map<ColumnMeta, Object> columnValues = EntityUtil.findAllColumns(entity);
        int index = 1;
        for (ColumnMeta column : columns) {
            if (column.getSqlType() == null) {
                throw new IllegalStateException("Unable to get SQL type from DB.");
            }
            statement.setObject(index, columnValues.get(column), column.getSqlType());
            index += 1;
        }
        statement.addBatch();
    }

    /**
     * 値をプロパティの型に合わせて変換する。
     * @param value 変換対象の値
     * @param column カラムのメタ情報
     * @return 変換した値
     */
    private Object convertToPropertyType(final Object value, ColumnMeta column) {
        return dialect.convertFromDatabase(value, column.getPropertyType());
    }

    /**
     * エンティティクラス名からテーブル名へ変換する。
     *
     * @param entity エンティティ
     * @param <T> エンティティクラス
     * @return テーブル名
     */
    public <T> String tableName(final T entity) {
        final Class<?> entityClass = entity.getClass();
        if (entityClass.getAnnotation(Entity.class) == null) {
            throw new IllegalEntityException(entityClass + " isn't a entity class.");
        }
        return EntityUtil.getTableName(entityClass);
    }

    @Override
    public DaoContext page(final long page) {
        this.page = page;
        if (per == null) {
            per = DEFAULT_PER;
        }
        return this;
    }

    @Override
    public DaoContext per(final long per) {
        this.per = per;
        return this;
    }

    @Override
    public DaoContext defer() {
        this.defer = true;
        return this;
    }

    /**
     * SQL_IDにファイル名がついてない場合は、Entityクラスの完全修飾名を付加する。
     *
     * @param <T> エンティティ型
     * @param sqlId SQL ID
     * @param entityClass エンティティクラス
     * @return 正規化したSQL ID
     */
    protected <T> String normalizeSqlId(final String sqlId, final Class<T> entityClass) {
        if (sqlId.contains("#")) {
            return sqlId;
        } else {
            return entityClass.getName() + '#' + sqlId;
        }
    }

    /**
     * IDジェネレータを設定する。
     *
     * @param type IDジェネレータのタイプ
     * @param generator IDジェネレータ
     */
    protected void setIdGenerator(final GenerationType type, final IdGenerator generator) {
        idGenerators.put(type, generator);
    }

    /**
     * 使用するデータベースコネクションを設定する。
     *
     * @param dbConnection データベースコネクション
     */
    protected void setDbConnection(final AppDbConnection dbConnection) {
        this.dbConnection = dbConnection;
    }
}

