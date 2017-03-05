package com.exem.bigdata.template.spark;

import com.exem.bigdata.template.spark.util.AbstractJob;
import com.exem.bigdata.template.spark.util.DateUtils;
import com.exem.bigdata.template.spark.util.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.exem.bigdata.template.spark.util.Constants.APP_FAIL;

public final class JavaSparkPi extends AbstractJob {

    private Map<String, String> params;

    @Override
    protected SparkSession setup(String[] args) throws Exception {
        addOption("slices", "s", "슬라이스", "2");
        addOption("appName", "n", "Spark Application", "Spark Application (" + DateUtils.getCurrentDateTime() + ")");

        params = parseArguments(args);
        if (params == null || params.size() == 0) {
            System.exit(APP_FAIL);
        }
        return SparkUtils.getSparkSessionForLocal(params.get("--appName"));
    }

    @Override
    protected void processing(SparkSession spark) throws Exception {
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        int slices = Integer.parseInt(params.get("--slices"));
        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y <= 1) ? 1 : 0;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        System.out.println("Pi is roughly " + 4.0 * count / n);
    }

    public static void main(String[] args) throws Exception {
        new JavaSparkPi().run(args);
    }

}
