package nablarch.common.dao;

import nablarch.core.db.support.ListSearchInfo;
import nablarch.core.util.annotation.Published;

/**
 * ページネーションのための値をもつクラス。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class Pagination extends ListSearchInfo {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** デフォルトコンストラクタ */
    Pagination() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * 本実装では、サポートしない。
     *
     * @throws UnsupportedOperationException
     */
    @Deprecated
    @Override
    public String[] getSearchConditionProps() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("getSearchConditionProps");
    }
}
