package nablarch.common.dao;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import nablarch.core.db.statement.SqlRow;
import nablarch.core.util.annotation.Published;

/**
 * 遅延Entityリストを表すクラス。
 * <p/>
 * 本クラスでは、データベースの検索結果をクライアントカーソルとして保持するのではなくサーバサイドカーソルとして保持する。
 * そのため、必要な処理が終了したタイミングで{@link #close()}メソッドを使用し、リソース解放を行うこと。
 * <p/>
 * 検索結果は、{@link #iterator()}で取得した{@link java.util.Iterator}を用いて取得する。
 * {@link java.util.Iterator#next()}を呼び出したタイミングで、
 * {@link java.sql.ResultSet#next()}を呼び出し次レコードの値を返却する。
 * <p/>
 * {@link #iterator()}の複数回呼び出しはサポートしない。
 * これは、{@link java.sql.ResultSet#TYPE_FORWARD_ONLY}のカーソルしかサポートしないため、
 * 一度読み込んだレコードを再度読み込むことは出来ないためである。
 * <p/>
 * 本クラスでは、{@link #iterator()}のみサポートする。
 * これ以外のメソッドが呼び出された場合は、{@link java.lang.UnsupportedOperationException}を送出する。
 *
 * @param <E> 型パラメータ
 * @author kawasima
 * @author Hisaaki Shioiri
 */
@Published(tag = "architect")
public class DeferredEntityList<E> extends EntityList<E> implements Closeable {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** SQL情報 */
    private final transient SqlResourceHolder resourceHolder;

    /** エンティティクラス */
    private final Class<E> entityClass;

    /**
     * 遅延EntityListを生成する。
     *
     * @param entityClass Entityのクラス
     * @param resourceHolder SQLリソース
     */
    public DeferredEntityList(Class<E> entityClass, SqlResourceHolder resourceHolder) {
        this.entityClass = entityClass;
        this.resourceHolder = resourceHolder;
    }

    @Published
    @Override
    public Iterator<E> iterator() {
        final Iterator<SqlRow> iter = resourceHolder.getResultSetIterator().iterator();
        return new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public E next() {
                SqlRow row = iter.next();
                if (entityClass.equals(SqlRow.class)) {
                    @SuppressWarnings("unchecked")
                    E e = (E) row;
                    return e;
                } else {
                    return EntityUtil.createEntity(entityClass, row);
                }
            }

            @Override
            public void remove() {
                iter.remove();
            }
        };
    }

    @Override
    public void close() {
        dispose();
    }

    /**
     * SQLリソースを解放する。
     */
    private void dispose() {
        resourceHolder.dispose();
    }

    /**
     * 本メソッドは利用できない。
     * 
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public ListIterator<E> listIterator() {
        throw new UnsupportedOperationException("listIterator");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public ListIterator<E> listIterator(int index) {
        throw new UnsupportedOperationException("listIterator");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public boolean add(E e) {
        throw new UnsupportedOperationException("add");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException("addAll");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException("clear");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("contains");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public void ensureCapacity(int minCapacity) {
        throw new UnsupportedOperationException("ensureCapacity");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public E get(int index) {
        throw new UnsupportedOperationException("get");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException("indexOf");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("isEmpty");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException("lastIndexOf");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("remove");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    protected void removeRange(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("removeRange");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public int size() {
        throw new UnsupportedOperationException("size");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("toArray");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException("toArray");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public void trimToSize() {
        throw new UnsupportedOperationException("trimToSize");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("subList");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("containsAll");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("removeAll");
    }

    /**
     * 本メソッドは利用できない。
     *
     * 呼び出した場合、{@link UnsupportedOperationException}を送出する。
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("retainAll");
    }

    @Override
    public String toString() {
        return "DeferredEntityList";
    }
}

