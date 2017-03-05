package com.exem.bigdata.template.spark.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Set;

/**
 * @see <a href="http://spark.apache.org/docs/latest/configuration.html>Spark Conf</a>
 */
public class SparkUtils {

    public static SparkSession getSparkSession(String appName, String master, boolean enableHiveSupport, SparkConf conf, Map<String, Object> confs) {
        SparkSession.Builder builder = SparkSession.builder();
        if (!StringUtils.isEmpty(appName)) builder.appName(appName);
        if (!StringUtils.isEmpty(master)) builder.master(master);
        if (enableHiveSupport) builder.enableHiveSupport();
        if (conf != null) builder.config(conf);
        if (confs != null) {
            Set<String> keys = confs.keySet();
            for (String key : keys) {
                Object value = confs.get(key);
                if (value instanceof Boolean) builder.config(key, (Boolean) value);
                if (value instanceof Double) builder.config(key, (Double) value);
                if (value instanceof Long) builder.config(key, (Long) value);
                if (value instanceof String) builder.config(key, (String) value);
            }
        }
        return builder.getOrCreate();
    }

    public static SparkSession getSparkSessionForMaster(String appName, String master) {
        return getSparkSession(appName, master, false, null, null);
    }

    public static SparkSession getSparkSessionForMaster(String appName, String master, SparkConf conf) {
        return getSparkSession(appName, master, false, conf, null);
    }

    public static SparkSession getSparkSessionForMaster(String appName, String masterIP, int masterPort) {
        return getSparkSession(appName, String.format("spark://%s:%s", masterIP, masterPort), false, null, null);
    }

    public static SparkSession getSparkSessionForMaster(String appName, String masterIP, int masterPort, SparkConf conf) {
        return getSparkSession(appName, String.format("spark://%s:%s", masterIP, masterPort), false, null, null);
    }

    public static SparkSession getSparkSessionForYARN(String appName) {
        return getSparkSession(appName, "yarn", false, null, null);
    }

    public static SparkSession getSparkSessionForYARN(String appName, SparkConf conf) {
        return getSparkSession(appName, "yarn", false, conf, null);
    }

    public static SparkSession getSparkSessionForLocal(String appName, int threads) {
        return getSparkSession(appName, "local[" + threads + "]", false, null, null);
    }

    public static SparkSession getSparkSessionForLocal(String appName) {
        return getSparkSession(appName, "local[*]", false, null, null);
    }
}
