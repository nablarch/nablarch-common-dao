package nablarch.common.dao;

import nablarch.core.util.annotation.Published;

/**
 * Entityの定義が誤っている場合に発生させる例外クラス。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class IllegalEntityException extends RuntimeException {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * 例外を生成します。
     *
     * @param msg 例外のメッセージ
     */
    public IllegalEntityException(String msg) {
        super(msg);
    }
}
