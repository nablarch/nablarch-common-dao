package nablarch.common.dao;

import nablarch.core.repository.ObjectLoader;
import nablarch.core.repository.SystemRepository;
import nablarch.test.support.log.app.OnMemoryLogWriter;
import org.junit.After;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

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
    public void tearDown() {
        setSystemRepositoryParamHideCauseExceptionLog(false);
        clearLog();
    }

    @Test
    public void testEquals() {
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
    public void testShowCauseExceptionLog() {
        try (final MockedStatic<DatabaseUtil> mocked = mockStatic(DatabaseUtil.class)) {
            mocked.when(() -> DatabaseUtil.getPrimaryKey(anyString())).thenThrow(new RuntimeException("Dummy exception by mock"));

            setSystemRepositoryParamHideCauseExceptionLog(false);
            new EntityMeta(EntityMetaTest.class); //内部でエラーが発生し、エラーログが出力される
            OnMemoryLogWriter.assertLogContains(WRITER_NAME,
                    "WARN Failed to process sortIdColumns.",
                    "java.lang.RuntimeException: Dummy exception by mock");
        }
    }

    @Test
    public void testHideCauseExceptionLog() {
        try (final MockedStatic<DatabaseUtil> mocked = mockStatic(DatabaseUtil.class)) {
            mocked.when(() -> DatabaseUtil.getPrimaryKey(anyString())).thenThrow(new RuntimeException("Dummy exception by mock"));

            setSystemRepositoryParamHideCauseExceptionLog(true);
            new EntityMeta(EntityMetaTest.class); //内部でエラーが発生するが、エラーログは出力されない
            assertNotLogContains(WRITER_NAME,
                    "WARN Failed to process sortIdColumns.",
                    "java.lang.RuntimeException: Dummy exception by mock");
        }
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