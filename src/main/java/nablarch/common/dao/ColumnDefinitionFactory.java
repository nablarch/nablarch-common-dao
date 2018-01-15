package nablarch.common.dao;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

/**
 * ColumnDefinitionを生成するインターフェース。
 *
 * @author Ryota Yoshinouchi
 */
interface ColumnDefinitionFactory {
    /**
     * ColumnDefinitionを生成する。
     * @param tableName テーブル名
     * @param propertyDescriptor プロパティの情報
     * @return ColumnDefinition
     */
    ColumnDefinition create(String tableName, final PropertyDescriptor propertyDescriptor);
}

/**
 * fieldを元にColumnDefinitionを生成するクラス。
 *
 * @author Ryota Yoshinouchi
 */
class FieldBasedColumnDefinitionFactory implements ColumnDefinitionFactory {

    @Override
    public ColumnDefinition create(final String tableName, final PropertyDescriptor propertyDescriptor) {
        final String name = propertyDescriptor.getName();
        final Class<?> entityClass = propertyDescriptor.getReadMethod().getDeclaringClass();
        final Field field;
        try {
            field = entityClass.getField(name);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("no field that corresponds to the property name. property name: " + name, e);
        }
        return new ColumnDefinition(tableName, propertyDescriptor, field.getAnnotations());
    }
}

/**
 * getterを元にColumnDefinitionを生成するクラス。
 *
 * @author Ryota Yoshinouchi
 */
class GetterBasedColumnDefinitionFactory implements ColumnDefinitionFactory {

    @Override
    public ColumnDefinition create(final String tableName, final PropertyDescriptor propertyDescriptor) {
        return new ColumnDefinition(tableName, propertyDescriptor, propertyDescriptor.getReadMethod().getAnnotations());
    }
}