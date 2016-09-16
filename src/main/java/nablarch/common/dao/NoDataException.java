package nablarch.common.dao;

import nablarch.core.util.annotation.Published;

/**
 * データが存在しないことを表す例外クラス。
 * <p/>
 * データが取得できるはずなのに取得出来なかった場合に発生する例外。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published
public class NoDataException extends RuntimeException {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;
}
