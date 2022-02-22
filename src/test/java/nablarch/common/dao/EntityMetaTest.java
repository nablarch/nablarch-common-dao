package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;

import nablarch.core.repository.ObjectLoader;
import nablarch.core.repository.SystemRepository;
import nablarch.core.util.Builder;
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

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    @Before
    public void setUp() throws Exception {
        clearLog();
        new MockUp<EntityMeta>() {
            @Mock
            public void sortIdColumns() {
                throw new RuntimeException("Dummy exception by mock");
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        setSystemRepositoryParamShowCauseExceptionLog(false); //デフォルトの状態=falseに戻す
        clearLog();
    }

    @Test
    public void testEquals() throws Exception {
        EntityMeta entityMeta = new EntityMeta(EntityMetaTest.class);

        assertThat(entityMeta.equals(null), is(false));
        assertThat(entityMeta.equals(""), is(false));
        assertThat(entityMeta.equals(entityMeta), is(true));
        assertThat(entityMeta.equals(entityMeta), is(true));
    }

    @Test
    public void testShowCauseExceptionLog() throws Exception {
        setSystemRepositoryParamShowCauseExceptionLog(true);
        new EntityMeta(EntityMetaTest.class); //内部でエラーが発生し、エラーログが出力される
        assertLog("WARN Failed to process sortIdColumns.",
                "Stack Trace Information : ",
                "java.lang.RuntimeException: Dummy exception by mock");
    }

    @Test
    public void testNotCauseExceptionLog() throws Exception {
        setSystemRepositoryParamShowCauseExceptionLog(false);
        new EntityMeta(EntityMetaTest.class); //内部でエラーが発生するが、エラーログは出力されない
        assertNotLog("Exception");
    }

    /**
     * システムリポジトリのパラメータnablarch.entityMeta.showCauseExceptionLogにparamで指定した値をセットする
     * @param param セットする値
     */
    private void setSystemRepositoryParamShowCauseExceptionLog(final boolean param) {
        SystemRepository.load(new ObjectLoader() {
            @Override
            public Map<String, Object> load() {
                final Map<String, Object> map = new HashMap<String, Object>();
                map.put("nablarch.entityMeta.showCauseExceptionLog", String.valueOf(param));
                return map;
            }
        });
    }

    /**
     * 期待する文言が全てログ出力されていることを確認する。
     *
     * @param expected 期待する文言（複数指定可）
     */
    private void assertLog(String... expected) {
        OnMemoryLogWriter.assertLogContains(WRITER_NAME, expected);
    }

    /**
     * 指定した文言がログに出力されていないことを確認する。
     * @param notExpected 出力されてはいけない文言
     */
    private void assertNotLog(String notExpected) {
        List<String> actualLogs = OnMemoryLogWriter.getMessages(WRITER_NAME);
        for (String actualLog : actualLogs) {
            if (actualLog.contains(notExpected)) {
                throw new AssertionError(Builder.concat(
                        "not expected log found. \n",
                        "don`t include = ", notExpected.toString(), "\n",
                        "actual = ", actualLogs.toString()));
            }
        }
    }

    /** ログを明示的にクリアする。 */
    private void clearLog() {
        OnMemoryLogWriter.clear();
    }
}