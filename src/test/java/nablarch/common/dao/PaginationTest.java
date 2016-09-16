package nablarch.common.dao;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.*;

import org.junit.Test;

/**
 * {@link nablarch.common.dao.Pagination}のテストクラス。
 */
public class PaginationTest {

    /**
     * インスタンスが作れること
     *
     * @throws Exception
     */
    @Test
    public void newInstance() throws Exception {
        Pagination pagination = new Pagination();
        assertThat("インスタンスが作れること", pagination, is(notNullValue()));
    }

    /**
     * getSearchConditionPropsは未サポートであること
     *
     * @throws Exception
     */
    @Test(expected = UnsupportedOperationException.class)
    public void getSearchConditionProps() throws Exception {
        Pagination pagination = new Pagination();
        pagination.getSearchConditionProps();
    }
}