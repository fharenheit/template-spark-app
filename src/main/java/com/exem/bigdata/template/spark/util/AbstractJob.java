package com.exem.bigdata.template.spark.util;


import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.*;

public abstract class AbstractJob implements Serializable {

    /**
     * MapReduce Job을 동작시키기 위한 파라미터의 Key Value Map.
     */
    private static Map<String, String> argMap;

    /**
     * 내부적으로 사용하기 위한 옵션 목록.
     */
    private static List<Option> options = new LinkedList<Option>();

    /**
     * Spark Conf
     */
    private static SparkConf sparkConf;

    /**
     * 기본 생성자.
     */
    protected AbstractJob() {
    }

    public void run(String[] args) throws Exception {
        SparkSession session = setup(args);
        processing(session);
        cleanup(session);
    }

    abstract protected SparkSession setup(String[] args) throws Exception;

    abstract protected void processing(SparkSession session) throws Exception;

    protected void cleanup(SparkSession session) throws Exception {
        session.stop();
    }

    /**
     * 인자가 없는 옵션을 추가한다. 인자가 없으므로 키가 포함되어 있는지 여부로 존재 여부를 판단한다.
     *
     * @param name        파라미터명(예; <tt>inputPath</tt>)
     * @param shortName   출약 파라미터명(예; <tt>i</tt>)
     * @param description 파라미터에 대한 설명문
     */
    protected void addFlag(String name, String shortName, String description) {
        options.add(buildOption(name, shortName, description, false, false, null));
    }

    /**
     * 기본값이 없는 옵션을 추가한다. 이 옵션은 필수 옵션이 아니다.
     *
     * @param name        파라미터명(예; <tt>inputPath</tt>)
     * @param shortName   출약 파라미터명(예; <tt>i</tt>)
     * @param description 파라미터에 대한 설명문
     */
    protected void addOption(String name, String shortName, String description) {
        options.add(buildOption(name, shortName, description, true, false, null));
    }

    /**
     * Hadoop MapReduce에 옵션을 추가한다. 옵션 추가는 커맨드 라인을 통해서 가능하며
     * 커맨드 라인은 {@link #parseArguments(String[])} 메소드를 호출하여 파싱하게 된다.
     *
     * @param name        파라미터명(예; <tt>inputPath</tt>)
     * @param shortName   출약 파라미터명(예; <tt>i</tt>)
     * @param description 파라미터에 대한 설명문
     * @param required    이 값이 <tt>true</tt>인 경우 커맨드 라인에서 입력 파라미터를 지정하지 않는 경우
     *                    예외를 발생시킨다. 사용법과 에러 메시지를 포함한 예외를 던진다.
     */
    protected static void addOption(String name, String shortName, String description, boolean required) {
        options.add(buildOption(name, shortName, description, true, required, null));
    }

    /**
     * Hadoop MapReduce에 옵션을 추가한다. 옵션 추가는 커맨드 라인을 통해서 가능하며
     * 커맨드 라인은 {@link #parseArguments(String[])} 메소드를 호출하여 파싱하게 된다.
     *
     * @param name         파라미터명(예; <tt>inputPath</tt>)
     * @param shortName    출약 파라미터명(예; <tt>i</tt>)
     * @param description  파라미터에 대한 설명문
     * @param defaultValue 커맨드 라인에서 입력 인자를 지정하지 않는 경우 설정할 기본값으로써 null을 허용한다.
     */
    protected static void addOption(String name, String shortName, String description, String defaultValue) {
        options.add(buildOption(name, shortName, description, true, false, defaultValue));
    }

    /**
     * Hadoop MapReduce에 옵션을 추가한다. 옵션 추가는 커맨드 라인을 통해서 가능하며
     * 커맨드 라인은 {@link #parseArguments(String[])} 메소드를 호출하여 파싱하게 된다.
     * 만약에 옵션이 인자를 가지고 있지 않다면 {@code parseArguments} 메소드 호출을 통해 리턴한
     * map의 {@code containsKey} 메소드를 통해서 옵션의 존재 여부를 확인하게 된다.
     * 그렇지 않은 경우 옵션의 영문 옵션명 앞에 '--'을 붙여서 map에서 해당 키가 존재하는지 확인한 후
     * 존재하는 경우 해당 옵션의 문자열값을 사용한다.
     *
     * @param option 추가할 옵션
     * @return 추가한 옵션
     */
    protected static Option addOption(Option option) {
        options.add(option);
        return option;
    }

    /**
     * 지정한 파라미터를 가진 옵션을 구성한다. 이름과 설명은 필수로 입력해야 한다.
     * required.
     *
     * @param name         커맨드 라인에서 '--'을 prefix로 가진 옵션의 이름
     * @param shortName    커맨드 라인에서 '--'을 prefix로 가진 옵션의 짧은 이름
     * @param description  도움말에 출력할 옵션에 대한 상세 설명
     * @param hasArg       인자를 가진다면 <tt>true</tt>
     * @param required     필수 옵션이라면 <tt>true</tt>
     * @param defaultValue 인자의 기본값. <tt>null</tt>을 허용한다.
     * @return 옵션
     */
    public static Option buildOption(String name,
                                     String shortName,
                                     String description,
                                     boolean hasArg,
                                     boolean required,
                                     String defaultValue) {
        DefaultOptionBuilder optBuilder = new DefaultOptionBuilder().withLongName(name).withDescription(description)
                .withRequired(required);
        if (shortName != null) {
            optBuilder.withShortName(shortName);
        }
        if (hasArg) {
            ArgumentBuilder argBuilder = new ArgumentBuilder().withName(name).withMinimum(1).withMaximum(1);
            if (defaultValue != null) {
                argBuilder = argBuilder.withDefault(defaultValue);
            }
            optBuilder.withArgument(argBuilder.create());
        }
        return optBuilder.create();
    }

    /**
     * 사용자가 입력한 커맨드 라인을 파싱한다.
     * 만약에 <tt>-h</tt>를 지정하거나 예외가 발생하는 경우 도움말을 출력하고 <tt>null</tt>을 반환한다.
     *
     * @param args 커맨드 라인 옵션
     * @return 인자와 인자에 대한 값을 포함하는 {@code Map<String,String>}.
     * 인자의 key는 옵션명에 되며 옵션명은 '--'을 prefix로 갖는다.
     * 따라서 옵션을 기준으로 {@code Map<String,String>} 에서 찾고자 하는 경우 반드시 옵션명에 '--'을 붙이도록 한다.
     */
    public static Map<String, String> parseArguments(String[] args) throws Exception {
        Option helpOpt = addOption(DefaultOptionCreator.helpOption());
        GroupBuilder groupBuilder = new GroupBuilder().withName("Spark Job 옵션:");
        for (Option opt : options) {
            groupBuilder = groupBuilder.withOption(opt);
        }
        Group group = groupBuilder.create();
        CommandLine cmdLine;
        try {
            Parser parser = new Parser();
            parser.setGroup(group);
            parser.setHelpOption(helpOpt);
            cmdLine = parser.parse(args);
        } catch (OptionException e) {
            System.err.println(e.getMessage());
            CommandLineUtil.printHelpWithGenericOptions(group, e);
            return null;
        }
        if (cmdLine.hasOption(helpOpt)) {
            CommandLineUtil.printHelpWithGenericOptions(group);
            return null;
        }
        argMap = new TreeMap<String, String>();
        maybePut(argMap, cmdLine, options.toArray(new Option[options.size()]));
        System.out.println("Command line arguments: ");
        Set<String> keySet = argMap.keySet();
        for (Iterator<String> iterator = keySet.iterator(); iterator.hasNext(); ) {
            String key = iterator.next();
            System.out.println("   " + key + " = " + argMap.get(key));
        }
        return argMap;
    }

    /**
     * 지정한 파라미터의 Boolean 값을 반환한다.
     *
     * @param key          커맨드 라인 파라미터
     * @param defaultValue Boolean 기본값
     * @return Boolean 값
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        if (StringUtils.isEmpty(argMap.get(keyFor(key)))) {
            return defaultValue;
        }
        try {
            return Boolean.parseBoolean(argMap.get(keyFor(key)));
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    /**
     * 지정한 파라미터의 Integer 값을 반환한다.
     *
     * @param key          커맨드 라인 파라미터
     * @param defaultValue Integer 기본값
     * @return Integer 값
     */
    public int getInt(String key, int defaultValue) {
        if (StringUtils.isEmpty(argMap.get(keyFor(key)))) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(argMap.get(keyFor(key)));
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    /**
     * 지정한 파라미터의 Float 값을 반환한다.
     *
     * @param key          커맨드 라인 파라미터
     * @param defaultValue Float 기본값
     * @return Float 값
     */
    public float getFloat(String key, float defaultValue) {
        if (StringUtils.isEmpty(argMap.get(keyFor(key)))) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(argMap.get(keyFor(key)));
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    /**
     * 지정한 파라미터의 Double 값을 반환한다.
     *
     * @param key          커맨드 라인 파라미터
     * @param defaultValue Double 기본값
     * @return Double 값
     */
    public double getDouble(String key, double defaultValue) {
        if (StringUtils.isEmpty(argMap.get(keyFor(key)))) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(argMap.get(keyFor(key)));
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    /**
     * 지정한 파라미터의 String 값을 반환한다.
     *
     * @param key          커맨드 라인 파라미터
     * @param defaultValue String 기본값
     * @return String 값
     */
    public String getString(String key, String defaultValue) {
        if (StringUtils.isEmpty(argMap.get(keyFor(key)))) {
            return defaultValue;
        }
        return getString(key);
    }

    /**
     * 지정한 파라미터의 String 값을 반환한다.
     *
     * @param key 커맨드 라인 파라미터
     * @return String 값
     */
    public String getString(String key) {
        return argMap.get(keyFor(key));
    }

    /**
     * 지정한 옵션명에 대해서 옵션 키를 구성한다. 예를 들여 옵션명이 <tt>name</tt> 이라면 실제 옵션 키는 <tt>--name</tt>이 된다.
     *
     * @param optionName 옵션명
     */
    public static String keyFor(String optionName) {
        return "--" + optionName;
    }

    /**
     * 지정한 옵션에 대해서 Option 객체를 반환한다.
     *
     * @return 요청한 옵션이 존재하는 경우 <tt>Option</tt>, 그렇지 않다면 <tt>null</tt>을 반환한다.
     */
    public String getOption(String optionName) {
        return argMap.get(keyFor(optionName));
    }

    /**
     * 지정한 옵션이 존재하는지 확인한다.
     *
     * @return 요청한 옵션이 존재하는 경우 <tt>true</tt>
     */
    public boolean hasOption(String optionName) {
        return argMap.containsKey(keyFor(optionName));
    }

    protected static void maybePut(Map<String, String> args, CommandLine cmdLine, Option... opt) {
        for (Option o : opt) {
            // 커맨드 라인에 옵션이 있거나 기본값과 같은 값이 있는 경우
            if (cmdLine.hasOption(o) || cmdLine.getValue(o) != null) {
                // 커맨드 라인의 옵션에 값이 OK
                // nulls are ok, for cases where options are simple flags.
                Object vo = cmdLine.getValue(o);
                String value = vo == null ? null : vo.toString();
                args.put(o.getPreferredName(), value);
            }
        }
    }

    /**
     * 입력한 Key Value로 커맨드 라인 입력 옵션을 구성한다. 이 메소드는
     * Workflow Engine에서 넘어온 Key Value 형태의 Map을 커맨드 라인으로 구성하기 위해서 사용하며 그 이외의 경우
     * 사용할 필요가 없다.
     *
     * @param key   MapReduce Job의 파라미터 Key
     * @param value MapReduce Job의 파라미터 Key의 Value
     * @return 커맨드 라인 입력 옵션
     */
    public static String getKeyValuePair(String key, String value) {
        return keyFor(key) + " " + value;
    }

    public static SparkConf getSparkConf(String appName) {
        sparkConf = new SparkConf();
        sparkConf.setAppName(appName);
        return sparkConf;
    }
}