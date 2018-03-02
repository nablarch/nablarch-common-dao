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
     * 本実装では、サポートしない。
     *
     * 呼び出した場合{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public String[] getSearchConditionProps() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("getSearchConditionProps");
    }
}
