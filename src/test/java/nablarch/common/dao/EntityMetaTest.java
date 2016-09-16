package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import nablarch.core.db.connection.DbConnectionContext;

import org.junit.After;
import org.junit.Test;

/**
 * {@link EntityMeta}のテストクラス。
 * <p/>
 * ※{@link EntityMeta}のテストは、基本{@link EntityUtilTest}にて実施している。
 * 本クラスでは、{@link EntityUtilTest}では実施できないequalsのみテストを行っている。
 */
public class EntityMetaTest {

    @After
    public void tearDown() throws Exception {
        DbConnectionContext.removeConnection();
    }

    @Test
    public void testEquals() throws Exception {
        EntityMeta entityMeta = new EntityMeta(EntityMetaTest.class);

        assertThat(entityMeta.equals(null), is(false));
        assertThat(entityMeta.equals(""), is(false));
        assertThat(entityMeta.equals(entityMeta), is(true));
        assertThat(entityMeta.equals(entityMeta), is(true));
    }
}