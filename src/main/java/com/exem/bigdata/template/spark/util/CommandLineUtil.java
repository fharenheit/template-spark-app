package com.exem.bigdata.template.spark.util;


import org.apache.commons.cli.Options;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.util.HelpFormatter;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * 커맨드 라인 유틸리티.
 *
 * @author Byoung Gon, Kim
 * @since 0.1
 */
public final class CommandLineUtil {
    public static void printHelp(Group group) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setGroup(group);
        formatter.print();
    }

    public static void printHelpWithGenericOptions(Group group) throws IOException {
        Options ops = new Options();
        org.apache.commons.cli.HelpFormatter fmt = new org.apache.commons.cli.HelpFormatter();
        fmt.printHelp("<command> [Generic Options] [Job-Specific Options]", "Generic Options:", ops, "");
        PrintWriter pw = new PrintWriter(System.out, true);
        HelpFormatter formatter = new HelpFormatter();
        formatter.setGroup(group);
        formatter.setPrintWriter(pw);
        formatter.printHelp();
        formatter.setFooter("Spark Job을 실행하는데 필요한 HDFS 디렉토리를 지정하거나 로컬 파일 시스템의 디렉토리를 지정하십시오.");
        formatter.printFooter();
        pw.flush();
    }

    /**
     * 커맨드 라인의 사용법을 구성한다.
     *
     * @param group Option Group
     * @param oe    예외
     * @throws IOException
     */
    public static void printHelpWithGenericOptions(Group group, OptionException oe) throws IOException {
        Options ops = new Options();
        org.apache.commons.cli.HelpFormatter fmt = new org.apache.commons.cli.HelpFormatter();
        fmt.printHelp("<command> [Generic Options] [Job-Specific Options]", "Generic Options:", ops, "");
        PrintWriter pw = new PrintWriter(System.out, true);
        HelpFormatter formatter = new HelpFormatter();
        formatter.setGroup(group);
        formatter.setPrintWriter(pw);
        formatter.setException(oe);
        formatter.print();
        pw.flush();
    }
}