package nablarch.common.dao;

import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.persistence.Id;
import javax.persistence.Version;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(DatabaseTestRunner.class)
public class ColumnMetaTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /**
     * {@link ColumnMeta#equals(Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void equals() throws Exception {
        ColumnMeta entityId = EntityUtil.findIdColumns(Entity.class)
                .get(0);

        ColumnMeta entity2Id = EntityUtil.findIdColumns(Entity2.class)
                .get(0);

        EntityUtil.clearCache();

        assertThat("同一Entityの同一カラムの結果はtrue",
                entityId.equals(EntityUtil.findIdColumns(Entity.class)
                        .get(0)), is(true));

        assertThat("同一エンティティの異なるカラムはfalse", entityId.equals(EntityUtil.findVersionColumn(new Entity())), is(false));

        assertThat("カラム名は同じでもEntityが異なるのでfalse", entityId.equals(entity2Id), is(false));

        assertThat("nullとの比較はfalse", entityId.equals(null), is(false));
        assertThat("異なるオブジェクトとの比較はfalse", entityId.equals(""), is(false));
    }

    public static class Entity {

        @Id
        public Long getId() {
            return 0L;
        }

        public String getName() {
            return "";
        }

        @Version
        public Long getVersion() {
            return 0L;
        }
    }

    public static class Entity2 {

        @Id
        public Long getId() {
            return 0L;
        }

        public String getName() {
            return "";
        }
    }
}