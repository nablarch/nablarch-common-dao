package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;

import nablarch.core.repository.ObjectLoader;
import nablarch.core.repository.SystemRepository;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.ClassRule;
import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;

/**
 * {@link EntityMeta}のテストクラス。
 * <p/>
 * ※{@link EntityMeta}のテストは、基本{@link EntityUtilTest}にて実施している。
 * 本クラスでは、{@link EntityUtilTest}では実施できないequalsのみテストを行っている。
 */
public class EntityMetaTest {

    /** ログライター名 */
    private static final String WRITER_NAME = "writer.memory";

    @After
    public void tearDown() throws Exception {
        setSystemRepositoryParamHideCauseExceptionLog(false);
        clearLog();
    }

    @Test
    public void testEquals() throws Exception {
        EntityMeta entityMeta = new EntityMeta(TestEntity.class);
        EntityMeta another = new EntityMeta(TestEntityAnother.class);
        EntityMeta sameEntityMeta = new EntityMeta(TestEntity.class);

        assertEquals(entityMeta, sameEntityMeta);
        assertNotEquals(entityMeta,another);
        assertNotEquals(entityMeta,null);
        assertNotEquals(entityMeta,new Object());
    }

    private static class TestEntity {
        // nop
    }

    private static class TestEntityAnother {
        // nop
    }

    @Test
    public void testShowCauseExceptionLog() throws Exception {
        new MockUp<EntityMeta>() {
            @Mock
            public void sortIdColumns() {
                throw new RuntimeException("Dummy exception by mock");
            }
        };
        setSystemRepositoryParamHideCauseExceptionLog(false);
        new EntityMeta(EntityMetaTest.class); //内部でエラーが発生し、エラーログが出力される
        OnMemoryLogWriter.assertLogContains(WRITER_NAME,
                "WARN Failed to process sortIdColumns.",
                "java.lang.RuntimeException: Dummy exception by mock");
    }

    @Test
    public void testHideCauseExceptionLog() throws Exception {
        new MockUp<EntityMeta>() {
            @Mock
            public void sortIdColumns() {
                throw new RuntimeException("Dummy exception by mock");
            }
        };
        setSystemRepositoryParamHideCauseExceptionLog(true);
        new EntityMeta(EntityMetaTest.class); //内部でエラーが発生するが、エラーログは出力されない
        assertNotLogContains(WRITER_NAME,
                "WARN Failed to process sortIdColumns.",
                "java.lang.RuntimeException: Dummy exception by mock");
    }

    /**
     * システムリポジトリのパラメータnablarch.entityMeta.hideCauseExceptionLogにparamで指定した値をセットする
     * @param param セットする値
     */
    private void setSystemRepositoryParamHideCauseExceptionLog(final boolean param) {
        SystemRepository.load(new ObjectLoader() {
            @Override
            public Map<String, Object> load() {
                final Map<String, Object> map = new HashMap<String, Object>();
                map.put("nablarch.entityMeta.hideCauseExceptionLog", String.valueOf(param));
                return map;
            }
        });
    }

    /**
     * 指定した文言がログに出力されていないことを確認する。
     * @param name ログ名
     * @param notContainsLogs 出力されてはいけない文言
     */
    private void assertNotLogContains(String name, String... notContainsLogs) {
        List<String> actualLogs = OnMemoryLogWriter.getMessages(name);
        for (String notContainsLog : notContainsLogs) {
            for (String actualLog : actualLogs) {
                assertThat(actualLog, not(containsString(notContainsLog)));
            }
        }
    }

    /** ログを明示的にクリアする。 */
    private void clearLog() {
        OnMemoryLogWriter.clear();
    }
}