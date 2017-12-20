package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import nablarch.test.support.SystemRepositoryResource;

import org.junit.ClassRule;
import org.junit.Test;

/**
 * {@link EntityMeta}のテストクラス。
 * <p/>
 * ※{@link EntityMeta}のテストは、基本{@link EntityUtilTest}にて実施している。
 * 本クラスでは、{@link EntityUtilTest}では実施できないequalsのみテストを行っている。
 */
public class EntityMetaTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    @Test
    public void testEquals() throws Exception {
        EntityMeta entityMeta = new EntityMeta(EntityMetaTest.class);

        assertThat(entityMeta.equals(null), is(false));
        assertThat(entityMeta.equals(""), is(false));
        assertThat(entityMeta.equals(entityMeta), is(true));
        assertThat(entityMeta.equals(entityMeta), is(true));
    }
}