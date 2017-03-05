package com.exem.bigdata.template.spark.util;

import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;

/**
 * Spark Job의 기본 커맨드 라인 파라미터를 생성하는 생성기.
 *
 * @author Byoung Gon, Kim
 * @since 0.1
 */
public class DefaultOptionCreator {

    /**
     * 입력 경로 옵션명.
     */
    public static final String INPUT_OPTION = "input";

    /**
     * 출력 경로 옵션명.
     */
    public static final String OUTPUT_OPTION = "output";

    /**
     * 커맨드 라인에서 사용할 도움말 옵션을 생성한다.
     *
     * @return 도움말 옵션
     */
    public static Option helpOption() {
        return new DefaultOptionBuilder().withLongName("help")
                .withDescription("도움말을 출력합니다.").withShortName("h").create();
    }

    /**
     * 커맨드 라인에서 사용할 입력 경로 옵션을 생성한다.
     *
     * @return 입력 경로 옵션
     */
    public static DefaultOptionBuilder inputOption() {
        return new DefaultOptionBuilder()
                .withLongName(INPUT_OPTION)
                .withRequired(false)
                .withShortName("i")
                .withArgument(
                        new ArgumentBuilder().withName(INPUT_OPTION).withMinimum(1).withMaximum(1).create())
                .withDescription("Spark Job의 입력 경로");
    }

    /**
     * 커맨드 라인에서 사용할 출력 경로 옵션을 생성한다.
     *
     * @return 출력 경로 옵션
     */
    public static DefaultOptionBuilder outputOption() {
        return new DefaultOptionBuilder()
                .withLongName(OUTPUT_OPTION)
                .withRequired(false)
                .withShortName("o")
                .withArgument(
                        new ArgumentBuilder().withName(OUTPUT_OPTION).withMinimum(1)
                                .withMaximum(1).create())
                .withDescription("Spark Job의 출력 경로");
    }
}