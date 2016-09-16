package nablarch.common.dao;

import static nablarch.common.dao.NamingConversionUtil.camelize;
import static nablarch.common.dao.NamingConversionUtil.deCamelize;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * {@link NamingConversionUtil}のテストクラス。
 *
 * @author kawasima
 */
@RunWith(Theories.class)
public class NamingConversionUtilTest {

    public static class Fixture {

        private final String snakeCase;

        private final String upperCamel;

        private final String upperSnakeCase;

        public Fixture(String snakeCase, String upperCamel, String upperSnakeCase) {
            this.snakeCase = snakeCase;
            this.upperCamel = upperCamel;
            this.upperSnakeCase = upperSnakeCase;
        }
    }

    @DataPoints
    public static Fixture[] getSnakeCase() {
        return new Fixture[] {
                new Fixture("abc_abc_abc", "AbcAbcAbc", "ABC_ABC_ABC"),
                new Fixture("Aaa_Bbb_Ccc", "AaaBbbCcc", "AAA_BBB_CCC"),
                new Fixture(null, null, null),
                new Fixture("", "", ""),
                new Fixture("___", "", ""),
        };
    }

    @Theory
    public void testCamelize(Fixture fixture) {
        assertThat(camelize(fixture.snakeCase), is(fixture.upperCamel));
    }

    @Theory
    public void testDecamelize(Fixture fixture) throws Exception {
        assertThat(deCamelize(fixture.upperCamel), is(fixture.upperSnakeCase));
    }
}

