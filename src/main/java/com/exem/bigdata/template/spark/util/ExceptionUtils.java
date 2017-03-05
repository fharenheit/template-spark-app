package com.exem.bigdata.template.spark.util;


import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/**
 * Exception Utilty.
 *
 * @author Byoung Gon, Kim
 * @since 0.2
 */
public class ExceptionUtils {
    /**
     * 메시지 패턴과 파라미터를 기반으로 예외 메시지를 구성한다.
     *
     * @param message SLF4J 형식의 메시지 패턴
     * @param args    메시지 패턴의 인자와 매핑하는 파라미터값
     * @return 예외 메시지
     */
    public static String getMessage(String message, Object... args) {
        return MessageFormatter.arrayFormat(message, args).getMessage();
    }

    /**
     * 발생한 예외에 대해서 ROOT Cause를 반환한다.
     *
     * @param exception 발생한 Exception
     * @return ROOT Cause
     */
    public static Throwable getRootCause(Exception exception) {
        return org.apache.commons.lang.exception.ExceptionUtils.getRootCause(exception);
    }

    /**
     * 발생한 예외에 대해서 Full Stack Trace를 logger에 경로 레벨로 남긴다.
     *
     * @param exception Exception
     * @param logger    SLF4J Logger
     */
    public static void printFullStackTrace(Exception exception, Logger logger) {
        logger.warn(org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(exception));
    }

    /**
     * 발생한 예외에 대해서 Full Stack Trace를 반환한다.
     *
     * @param exception Exception
     * @return Full Stack Trace
     */
    public static String getFullStackTrace(Exception exception) {
        return org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(exception);
    }
}