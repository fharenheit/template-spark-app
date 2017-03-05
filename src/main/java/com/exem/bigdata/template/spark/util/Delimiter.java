package com.exem.bigdata.template.spark.util;


/**
 * 범용적으로 사용하는 구분자를 표현하는 Enumeration.
 *
 * @author Byoung Gon, Kim
 * @since 0.1
 */
public enum Delimiter {

    SPACE("\u0020"),
    TAB("\u0009"),
    PIPE("|"),
    DOUBLE_PIPE("||"),
    COMMA("\u002c"),
    PERIOD("."),
    SEMICOLON(";"),
    COLON(":"),
    ASTERISK("*"),
    HYPEN("-"),
    TILDE("~"),
    CROSSHATCH("#"),
    EXCLAMATION_MARK("!"),
    DOLLAR("$"),
    AMPERSAND("&"),
    PERCENT("%"),
    QUOTATION_MARK("\""),
    CIRCUMFLEX("^");

    /**
     * 컬럼을 구분하는 delimiter
     */
    private String delimiter;

    /**
     * Delimiter를 설정한다.
     *
     * @param delimiter delimiter
     */
    Delimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    /**
     * Delimiter를 반환한다.
     *
     * @return delimiter
     */
    public String getDelimiter() {
        return delimiter;
    }
}