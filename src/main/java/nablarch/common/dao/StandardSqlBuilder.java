package nablarch.common.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.Entity;

import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;

/**
 * ユニバーサルDAOで使用するSQL文を構築するクラス。
 * <p/>
 * 生成するSQL文は以下のとおり。
 * <p/>
 * <ul>
 * <li>ID列を条件としたSELECT文</li>
 * <li>条件なしのSELECT文</li>
 * <li>ID列を条件としたUPDATE文(バージョンカラムがある場合はそのカラムも条件に含まれる)</li>
 * <li>ID列を条件としたDELETE文</li>
 * <li>全カラムを対象としたINSERT文(IDENTITYカラムはサポートしない)</li>
 * </ul>
 *
 * @author hisaaki sioiri
 */
@Published(tag = "architect")
public class StandardSqlBuilder {

    /**
     * ID列を条件として全カラムの情報を取得するSQL文を構築する。
     *
     * @param entityClass エンティティクラス
     * @param <T> 型パラメータ
     * @return ID列を条件としたSQL文
     */
    public <T> String buildSelectByIdSql(final Class<T> entityClass) {
        return buildSelectAllSql(entityClass)
                + ' ' + buildIdCondition(entityClass);
    }

    /**
     * 全レコード検索(条件なし)のSQL文を構築する。
     *
     * @param entityClass エンティティクラス
     * @param <T> 型パラメータ
     * @return 条件なしのSELECT文
     */
    public <T> String buildSelectAllSql(final Class<T> entityClass) {
        final StringBuilder sql = new StringBuilder(512);
        sql.append("SELECT ");

        final List<ColumnMeta> columns = EntityUtil.findAllColumns(entityClass);
        final List<String> columnNames = new ArrayList<String>(columns.size());
        for (ColumnMeta column : columns) {
            columnNames.add(column.getName());
        }
        sql.append(StringUtil.join(",", columnNames));
        sql.append(" FROM ")
                .append(EntityUtil.getTableNameWithSchema(entityClass));
        return sql.toString();
    }

    /**
     * ID列を条件とした更新用のSQL文を構築する。
     *
     * @param entity エンティティ情報
     * @param <T> 型
     * @return 更新用のSQL文
     */
    public <T> SqlWithParams buildUpdateSql(final T entity) {
        final BatchSqlWithColumns sqlWithColumns = buildBatchUpdateSql(entity.getClass());

        final Map<ColumnMeta, Object> columns = EntityUtil.findAllColumns(entity);
        final List<Object> params = new ArrayList<Object>();

        for (ColumnMeta column : sqlWithColumns.getColumns()) {
            params.add(columns.get(column));
        }

        return new SqlWithParams(sqlWithColumns.getSql(), params);
    }

    /**
     * ID列を条件とした一括更新用(batch update)のSQL文を構築する。
     *
     * @param entityClass エンティティクラス
     * @param <T> 型
     * @return 更新用のSQL文
     */
    public <T> BatchSqlWithColumns buildBatchUpdateSql(final Class<T> entityClass) {
        final String tableName = toTableName(entityClass);

        ColumnMeta versionColumn = null;

        final List<ColumnMeta> columns = EntityUtil.findAllColumns(entityClass);
        final List<String> set = new ArrayList<String>();
        final List<ColumnMeta> bindColumns = new ArrayList<ColumnMeta>();

        for (ColumnMeta column : columns) {
            if (column.isIdColumn()) {
                continue;
            }
            final String columnName = column.getName();
            if (column.isVersion()) {
                set.add(columnName + '=' + columnName + "+1");
                versionColumn = column;
            } else {
                set.add(columnName + "=?");
                bindColumns.add(column);
            }
        }

        final List<ColumnMeta> idColumns = EntityUtil.findIdColumns(entityClass);

        final List<String> where = new ArrayList<String>();
        for (ColumnMeta column : idColumns) {
            where.add(column.getName() + "=?");
            bindColumns.add(column);
        }

        if (versionColumn != null) {
            where.add(versionColumn.getName() + "=?");
            bindColumns.add(versionColumn);
        }

        final StringBuilder sql = new StringBuilder(512);
        sql.append("UPDATE ")
                .append(tableName);
        sql.append(" SET ")
                .append(StringUtil.join(",", set))
                .append(' ')
                .append("WHERE ")
                .append(StringUtil.join(" AND ", where));

        return new BatchSqlWithColumns(sql.toString(), bindColumns);
    }

    /**
     * ID列を条件とした削除用のSQL文を構築する。
     *
     * @param entity エンティティ
     * @param <T> 型
     * @return 削除用SQL文
     */
    public <T> SqlWithParams buildDeleteSql(final T entity) {

        final BatchSqlWithColumns sqlWithColumns = buildBatchDeleteSql(entity.getClass());

        final Map<ColumnMeta, Object> columns = EntityUtil.findIdColumns(entity);

        final List<Object> params = new ArrayList<Object>();
        for (ColumnMeta column : sqlWithColumns.getColumns()) {
            params.add(columns.get(column));
        }
        return new SqlWithParams(sqlWithColumns.getSql(), params);
    }

    /**
     * ID列を条件とした一括削除用(batch delete)のSQL文を構築する。
     *
     * @param entityClass エンティティクラス
     * @param <T> エンティティクラス
     * @return 一括削除用SQL文
     */
    public <T> BatchSqlWithColumns buildBatchDeleteSql(final Class<T> entityClass) {
        final StringBuilder sql = new StringBuilder(512);

        final List<ColumnMeta> idColumns = EntityUtil.findIdColumns(entityClass);

        final List<String> whereClause = new ArrayList<String>(idColumns.size());
        for (ColumnMeta column : idColumns) {
            whereClause.add(column.getName() + "=?");
        }

        sql.append("DELETE FROM ")
                .append(toTableName(entityClass))
                .append(" WHERE ")
                .append(StringUtil.join(" AND ", whereClause));

        return new BatchSqlWithColumns(sql.toString(), idColumns);
    }

    /**
     * 登録用のSQLを構築する。
     * <p/>
     * 採番カラムも含む全てのカラムを対象としたデータ登録用INSERT文を構築する。
     * 構築したINSERT文を使用する場合には、採番されるカラムの値は事前に採番し、
     * Entityクラスに対して値を設定する必要がある。
     *
     * @param entity エンティティ
     * @param <T> エンティティクラス
     * @return 構築したSQL
     */
    public <T> SqlWithParams buildInsertSql(final T entity) {
        return buildInsertSql(entity, true);
    }

    /**
     * 登録用のSQLを構築する。
     * <p/>
     * データベース側での採番(MySqlのAUTO_INCREMENTやPostgreSqlのSERIALカラムなど)を行うための
     * データ登録用INSERT文を生成する。
     * 構築したINSERT文を使用する場合には、データベースへのデータ登録時に値が採番されるため、
     * データ登録後にデータベースから採番された値を取得する必要がある。
     *
     * @param entity エンティティ
     * @param <T> 型
     * @return 構築したSQL
     */
    public <T> SqlWithParams buildInsertWithIdentityColumnSql(final T entity) {
        return buildInsertSql(entity, false);
    }

    /**
     * 登録用のSQL文を構築する。
     *
     * @param entity エンティティ
     * @param includeGeneratedColumn 採番対象のカラムをSQL文に含めるかどうか({@code true}の場合は含める)
     * @param <T> エンティティクラス
     * @return 構築したSQL
     */
    private <T> SqlWithParams buildInsertSql(final T entity, final boolean includeGeneratedColumn) {
        final BatchSqlWithColumns sqlWithColumns = buildInsertSqlWithColumns(entity.getClass(), includeGeneratedColumn);

        final Map<ColumnMeta, Object> columnsWithParam = EntityUtil.findAllColumns(entity);
        final List<Object> params = new ArrayList<Object>();
        for (ColumnMeta column : sqlWithColumns.getColumns()) {
            params.add(columnsWithParam.get(column));
        }
        return new SqlWithParams(sqlWithColumns.getSql(), params);
    }

    /**
     * 一括登録用(batch insert用)のSQLを構築する。
     * <p/>
     * 採番カラムも含む全てのカラムを対象としたデータ登録用INSERT文を構築する。
     * 構築したINSERT文を使用する場合には、採番されるカラムの値は事前に採番し、
     * Entityクラスに対して値を設定する必要がある。
     *
     * @param entityClass エンティティクラス
     * @param <T> エンティティクラス
     * @return 構築したSQL
     */
    public <T> BatchSqlWithColumns buildBatchInsertSql(final Class<T> entityClass) {
        return buildInsertSqlWithColumns(entityClass, true);
    }

    /**
     * 一括登録用(batch insert用)のSQLを構築する。
     * <p/>
     * データベース側での採番(MySqlのAUTO_INCREMENTやPostgreSqlのSERIALカラムなど)を行うための
     * データ登録用INSERT文を生成する。
     * 構築したINSERT文を使用する場合には、データベースへのデータ登録時に値が採番されるため、
     * データ登録後にデータベースから採番された値を取得する必要がある。
     *
     * @param entityClass エンティティクラス
     * @param <T> エンティティクラス
     * @return 構築したSQL
     */
    public <T> BatchSqlWithColumns buildBatchInsertWithIdentityColumnSql(final Class<T> entityClass) {
        return buildInsertSqlWithColumns(entityClass, false);
    }

    /**
     * 一括登録用(batch insert)用のSQL文を構築する。
     *
     * @param entityClass エンティティクラス
     * @param includeGeneratedColumn 採番対象のカラムをSQL文に含めるかどうか({@code true}の場合は含める)
     * @param <T> エンティティクラス
     * @return 構築したSQL
     */
    private <T> BatchSqlWithColumns buildInsertSqlWithColumns(
            final Class<T> entityClass, final boolean includeGeneratedColumn) {

        final StringBuilder sql = new StringBuilder(512);
        sql.append("INSERT INTO ")
                .append(toTableName(entityClass))
                .append('(');

        final List<String> columnNames = new ArrayList<String>();
        final List<String> values = new ArrayList<String>();
        final List<ColumnMeta> columns = new ArrayList<ColumnMeta>();

        for (ColumnMeta column : EntityUtil.findAllColumns(entityClass)) {
            if (!includeGeneratedColumn && column.isGeneratedValue()) {
                continue;
            }
            columnNames.add(column.getName());
            values.add("?");
            columns.add(column);
        }
        sql.append(StringUtil.join(",", columnNames))
                .append(")VALUES(")
                .append(StringUtil.join(",", values))
                .append(')');

        return new BatchSqlWithColumns(sql.toString(), columns);
    }

    /**
     * ID列を条件とするWHERE句を構築する。
     *
     * @param entityClass エンティティクラス
     * @param <T> 型パラメータ
     * @return ID列を条件としたWHERE句
     */
    protected <T> String buildIdCondition(final Class<T> entityClass) {
        final List<ColumnMeta> idColumns = EntityUtil.findIdColumns(entityClass);

        final List<String> conditions = new ArrayList<String>(idColumns.size());
        for (ColumnMeta idColumn : idColumns) {
            conditions.add(idColumn.getName() + "=?");
        }

        return "WHERE " + StringUtil.join(" AND ", conditions);
    }

    /**
     * エンティティクラス名からテーブル名(スキーマ名つき)へ変換する。
     *
     * @param entityClass エンティティクラス
     * @param <T> エンティティクラス
     * @return スキーマ名を修飾子に指定したテーブル名(スキーマ名指定がない場合は、テーブル名のみ)
     */
    protected <T> String toTableName(final Class<T> entityClass) {
        if (entityClass.getAnnotation(Entity.class) == null) {
            throw new IllegalEntityException(entityClass + " isn't a entity class.");
        }
        return EntityUtil.getTableNameWithSchema(entityClass);
    }
}

