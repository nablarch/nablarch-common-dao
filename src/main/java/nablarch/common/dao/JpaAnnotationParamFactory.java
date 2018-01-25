package nablarch.common.dao;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

/**
 * {@link JpaAnnotationParam}を生成するインターフェース。
 *
 * @author Ryota Yoshinouchi
 */
interface JpaAnnotationParamFactory {
    /**
     * ColumnDefinitionを生成する。
     * @param tableName テーブル名
     * @param propertyDescriptor プロパティの情報
     * @param entityClass
     * @return JpaAnnotationParam
     */
    JpaAnnotationParam create(String tableName, final PropertyDescriptor propertyDescriptor, Class<?> entityClass);
}

/**
 * フィールドを元に{@link JpaAnnotationParam}を生成するクラス。
 *
 * @author Ryota Yoshinouchi
 */
class FieldBasedJpaAnnotationParamFactory implements JpaAnnotationParamFactory {

    @Override
    public JpaAnnotationParam create(final String tableName, final PropertyDescriptor propertyDescriptor, final Class<?> entityClass) {
        final String name = propertyDescriptor.getName();
        final Field field;
        try {
            field = entityClass.getField(name);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("no field that corresponds to the property name. entity class: " + entityClass.getName() + ", property name: " + name, e);
        }
        return new JpaAnnotationParam(tableName, propertyDescriptor, field.getAnnotations());
    }
}

/**
 * getterを元に{@link JpaAnnotationParam}を生成するクラス。
 *
 * @author Ryota Yoshinouchi
 */
class GetterBasedJpaAnnotationParamFactory implements JpaAnnotationParamFactory {

    @Override
    public JpaAnnotationParam create(final String tableName, final PropertyDescriptor propertyDescriptor, final Class<?> entityClass) {
        final Annotation[] annotations;
        try{
            annotations = propertyDescriptor.getReadMethod().getAnnotations();
        } catch(NullPointerException e) {
            throw new IllegalArgumentException("no getter that corresponds to the property. entity class: " + entityClass.getName() + ", property name: " + propertyDescriptor.getName(), e);
        }
        return new JpaAnnotationParam(tableName, propertyDescriptor, annotations);
    }
}