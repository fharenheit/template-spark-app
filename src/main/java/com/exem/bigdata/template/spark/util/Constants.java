package com.exem.bigdata.template.spark.util;

/**
 * Spark Application 에서 사용하는 각종 상수를 정의한 상수 클래스.
 *
 * @author Byoung Gon, Kim
 * @since 0.1
 */
public class Constants {

    /**
     * Default Delimiter
     */
    public static final String DEFAULT_DELIMITER = ",";

    /**
     * Hive Default Delimiter
     */
    public static final String DEFAULT_DELIMITER_HIVE = DEFAULT_DELIMITER;

    /**
     * Sqoop Default Delimiter
     */
    public static final String DEFAULT_DELIMITER_SQOOP = DEFAULT_DELIMITER;

    /**
     * Spark Application Fail
     */
    public static final int APP_FAIL = -1;

    /**
     * Spark Application Success
     */
    public static final int APP_SUCCESS = 0;
    public static final int NONE = -1;
    public static final int FALSE = 0;
    public static final int TRUE = 1;

    /**
     * YES
     */
    public static final String YES = "yes";

    /**
     * NO
     */
    public static final String NO = "no";

    /**
     * 현재 날짜를 표현하는 긴 문자열 형식의 밀리초 단위 날짜 포맷.
     */
    public static final String UTC_DATE_FORMAT = "yyyyMMddHHmmss";
}