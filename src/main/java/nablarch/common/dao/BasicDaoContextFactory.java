package nablarch.common.dao;

import javax.persistence.GenerationType;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.Dialect;

/**
 * {@link DaoContextFactory}の基本実装クラス。
 * <p/>
 * 本実装では、{@link BasicDaoContext}を生成する。
 * <p/>
 * {@link javax.persistence.GeneratedValue}で必要となる
 * {@link nablarch.common.idgenerator.IdGenerator}の実装をDIする必要がある。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
public class BasicDaoContextFactory extends DaoContextFactory {

    @Override
    public DaoContext create() {
        AppDbConnection appDbConnection = dbConnection.get();
        if (appDbConnection == null) {
            appDbConnection = DbConnectionContext.getConnection();
        }
        Dialect dialect = ((TransactionManagerConnection) appDbConnection).getDialect();

        final BasicDaoContext daoContext = new BasicDaoContext(sqlBuilder, dialect);
        daoContext.setDbConnection(appDbConnection);
        if (sequenceIdGenerator != null) {
            daoContext.setIdGenerator(GenerationType.SEQUENCE, sequenceIdGenerator);
        }
        if (tableIdGenerator != null) {
            daoContext.setIdGenerator(GenerationType.TABLE, tableIdGenerator);
        }

        return daoContext;
    }
}

