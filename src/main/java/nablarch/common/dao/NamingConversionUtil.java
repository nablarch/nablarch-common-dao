package nablarch.common.dao;

/**
 * 変数名やクラス名を相互に変換するユーティリティクラス。
 *
 * @author kawasima
 * @author Hisaaki Shioiri
 */
public final class NamingConversionUtil {

    /** 隠蔽コンストラクタ */
    private NamingConversionUtil() {
    }

    /**
     * 文字列をアッパーキャメル(パスカルケース)に変換する。
     * <p/>
     * 例:
     * <pre>
     * {@code
     * NamingConversionUtils.camelize("AAA_BBB_CCC");   -> AaaBbbCcc
     * }
     * </pre>
     *
     * @param value 文字列(スネークケースを想定)
     * @return 変換後の文字列
     */
    public static String camelize(String value) {
        if (value == null) {
            return null;
        }

        String[] tokens = value.split("_");
        StringBuilder camelized = new StringBuilder(value.length());
        for (String token : tokens) {
            for (int i = 0; i < token.length(); i++) {
                if (i == 0) {
                    camelized.append(Character.toUpperCase(token.charAt(i)));
                } else {
                    camelized.append(Character.toLowerCase(token.charAt(i)));
                }
            }
        }
        return camelized.toString();
    }

    /**
     * アーパーキャメル(パスカルケース)の文字列を全て大文字のスネークケースに変換する。
     * <p/>
     * 例:
     * <pre>
     * {@code
     * NamingConversionUtils.decamelize("AbcAbcAbc");   -> ABC_ABC_ABC
     * }
     * </pre>
     *
     * @param value 文字列(アッパーキャメルを想定)
     * @return 変換後の文字列
     */
    public static String deCamelize(String value) {
        if (value == null) {
            return null;
        }

        StringBuilder deCamelized = new StringBuilder(value.length() + 10);
        for (int i = 0; i < value.length(); i++) {
            if (Character.isUpperCase(value.charAt(i)) && deCamelized.length() > 0) {
                deCamelized.append('_');
            }
            deCamelized.append(Character.toUpperCase(value.charAt(i)));
        }
        return deCamelized.toString();
    }
}

