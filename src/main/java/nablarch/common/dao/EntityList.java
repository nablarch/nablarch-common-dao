package nablarch.common.dao;

import java.util.ArrayList;
import java.util.Collection;

import nablarch.core.util.annotation.Published;

/**
 * {@link UniversalDao}から返される結果リストの保持クラス。
 * <p/>
 * ページネーションのためのページ数や検索条件に一致した件数なども本クラスで保持する。
 *
 * @param <E> 型パラメータ
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published
public class EntityList<E> extends ArrayList<E> {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** ページング情報 */
    private Pagination pagination;

    /** デフォルトコンストラクタ */
    public EntityList() {
        super();
    }

    /**
     * 指定の初期容量でEntityListを生成する。
     *
     * @param initCapacity 初期容量
     * @see ArrayList#ArrayList(int)
     */
    public EntityList(final int initCapacity) {
        super(initCapacity);
    }

    /**
     * 指定のコレクションでEntityListを生成する。
     *
     * @param collection コレクション
     * @see ArrayList#addAll(Collection)
     */
    public EntityList(final Collection<? extends E> collection) {
        super(collection);
    }

    /**
     * ページ番号を設定する。
     *
     * @param page ページ番号
     */
    protected void setPage(final long page) {
        initPagination();
        pagination.setPageNumber((int) page);
    }

    /**
     * 検索結果の取得最大件数を設定する。
     *
     * @param max 取得最大件数
     */
    protected void setMax(final long max) {
        initPagination();
        pagination.setMax((int) max);
    }

    /**
     * 検索結果の総件数を設定する。
     *
     * @param resultCount 検索結果の総件数
     */
    protected void setResultCount(final long resultCount) {
        initPagination();
        pagination.setResultCount((int) resultCount);
    }

    /**
     * ページングのための情報を取得する。
     *
     * @return ページングの情報
     */
    public Pagination getPagination() {
        return pagination;
    }

    /**
     * ページングのための情報を初期化する。
     * <p/>
     * 既に初期化済みの場合には、何もしない。
     */
    private void initPagination() {
        if (pagination == null) {
            pagination = new Pagination();
        }
    }

    @Override
    @Deprecated
    public void add(final int index, final E element) {
        throw new UnsupportedOperationException("add");
    }

    @Override
    @Deprecated
    public boolean addAll(final int index, final Collection<? extends E> c) {
        throw new UnsupportedOperationException("addAll");
    }

    @Override
    @Deprecated
    public E remove(final int index) {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    @Deprecated
    public E set(final int index, final E element) {
        throw new UnsupportedOperationException("set");
    }
}

