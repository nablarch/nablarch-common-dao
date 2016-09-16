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

    @Override
    @Deprecated
    public ListIterator<E> listIterator() {
        throw new UnsupportedOperationException("listIterator");
    }

    @Override
    @Deprecated
    public ListIterator<E> listIterator(int index) {
        throw new UnsupportedOperationException("listIterator");
    }

    @Override
    @Deprecated
    public boolean add(E e) {
        throw new UnsupportedOperationException("add");
    }

    @Override
    @Deprecated
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException("addAll");
    }

    @Override
    @Deprecated
    public void clear() {
        throw new UnsupportedOperationException("clear");
    }

    @Override
    @Deprecated
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("contains");
    }

    @Override
    @Deprecated
    public void ensureCapacity(int minCapacity) {
        throw new UnsupportedOperationException("ensureCapacity");
    }

    @Override
    @Deprecated
    public E get(int index) {
        throw new UnsupportedOperationException("get");
    }

    @Override
    @Deprecated
    public int indexOf(Object o) {
        throw new UnsupportedOperationException("indexOf");
    }

    @Deprecated
    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("isEmpty");
    }

    @Override
    @Deprecated
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException("lastIndexOf");
    }

    @Override
    @Deprecated
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    @Deprecated
    protected void removeRange(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("removeRange");
    }

    @Override
    @Deprecated
    public int size() {
        throw new UnsupportedOperationException("size");
    }

    @Override
    @Deprecated
    public Object[] toArray() {
        throw new UnsupportedOperationException("toArray");
    }

    @Override
    @Deprecated
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException("toArray");
    }

    @Override
    @Deprecated
    public void trimToSize() {
        throw new UnsupportedOperationException("trimToSize");
    }

    @Override
    @Deprecated
    public List<E> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("subList");
    }

    @Override
    @Deprecated
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("containsAll");
    }

    @Override
    @Deprecated
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("removeAll");
    }

    @Override
    @Deprecated
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("retainAll");
    }

    @Override
    public String toString() {
        return "DeferredEntityList";
    }
}

