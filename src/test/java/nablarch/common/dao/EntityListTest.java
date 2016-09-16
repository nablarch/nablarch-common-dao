package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

/**
 * {@link EntityList}のテスクラス。
 */
public class EntityListTest {

    /**
     * ページングに関する情報のアクセッサのテスト。
     *
     * @throws Exception
     */
    @Test
    public void paging() throws Exception {
        EntityList<Object> sut = new EntityList<Object>();
        sut.setMax(100);
        sut.setPage(2);
        sut.setResultCount(500);
        Pagination pagination = sut.getPagination();
        assertThat(pagination.getMax(), is(100));
        assertThat(pagination.getPageNumber(), is(2));
        assertThat(pagination.getResultCount(), is(500));
    }

    /**
     * インスタンスの生成に関するテスト
     */
    @Test
    public void newInstance() {
        assertThat(new EntityList<Object>().size(), is(0));
        assertThat(new EntityList<Object>(20).size(), is(0));
        assertThat(new EntityList<Object>(new ArrayList<Object>() {{
            add("hoge");
            add("fuga");
        }}).size(), is(2));
    }

    /**
     * 位置指定の要素追加は出来ない。
     *
     * @throws Exception
     */
    @Test(expected = UnsupportedOperationException.class)
    public void add_index() throws Exception {
        new EntityList<Object>().add(0, new Object());
    }

    /**
     * 位置指定のaddAllは出来ない。
     *
     * @throws Exception
     */
    @Test(expected = UnsupportedOperationException.class)
    public void name() throws Exception {
        new EntityList<Object>().addAll(1, null);
    }

    /**
     * 要素の削除は出来ない
     *
     * @throws Exception
     */
    @Test(expected = UnsupportedOperationException.class)
    public void remove() throws Exception {
        new EntityList<Object>().remove(0);
    }

    /**
     * setは出来ない
     *
     * @throws Exception
     */
    @Test(expected = UnsupportedOperationException.class)
    public void set() throws Exception {
        new EntityList<Object>().set(0, null);
    }
}
