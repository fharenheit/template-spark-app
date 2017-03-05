package com.exem.bigdata.template.spark;

import com.exem.bigdata.template.spark.util.AbstractJob;
import com.exem.bigdata.template.spark.util.DateUtils;
import com.exem.bigdata.template.spark.util.SparkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.exem.bigdata.template.spark.util.Constants.APP_FAIL;

public final class JavaSparkProduct extends AbstractJob {

    private Map<String, String> params;

    @Override
    protected SparkSession setup(String[] args) throws Exception {
        addOption("appName", "n", "Spark Application", "Spark Application (" + DateUtils.getCurrentDateTime() + ")");

        params = parseArguments(args);
        if (params == null || params.size() == 0) {
            System.exit(APP_FAIL);
        }
        return SparkUtils.getSparkSessionForLocal(params.get("--appName"));
    }

    @Override
    protected void processing(SparkSession sparkSession) throws Exception {
        StructType schema = new StructType(
                new StructField[]{
                        new StructField("GROUPED_KEY", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("PRODUCT_CLASSIFICATION", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("PRODUCT_NM", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("BRAND_LINE", DataTypes.StringType, true, Metadata.empty())
                }
        );

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> stringRDD = jsc.textFile("product.txt").persist(StorageLevel.MEMORY_AND_DISK());
        JavaRDD<Product> products = stringRDD.map(new Function<String, Product>() {
            @Override
            public Product call(String row) throws Exception {
                String[] columns = StringUtils.splitPreserveAllTokens(row, ",");

                Product product = new Product();
                product.PRODUCT_CLASSIFICATION = columns[0];
                product.PRODUCT_NM = columns[1];
                product.BRAND_LINE = columns[2];
                product.GROUPED_KEY = product.PRODUCT_CLASSIFICATION + product.PRODUCT_NM + product.BRAND_LINE;

                return product;
            }
        }).cache();

        System.out.println(products.count());

        JavaPairRDD<String, Iterable<Product>> pairRDD = products.groupBy(new Function<Product, String>() {
            @Override
            public String call(Product p) throws Exception {
                return p.GROUPED_KEY;
            }
        });

        System.out.println(pairRDD.count());
        System.out.println(pairRDD.collectAsMap());

        JavaPairRDD<String, List<Product>> stringListJavaPairRDD = pairRDD.mapValues(new Function<Iterable<Product>, List<Product>>() {
            @Override
            public List<Product> call(Iterable<Product> products) throws Exception {
                List<Product> list = new ArrayList<Product>();
                Iterator<Product> iterator = products.iterator();
                while (iterator.hasNext()) {
                    Product product = iterator.next();
                    if (product.GROUPED_KEY.equals("101112")) list.add(product);
                }
                return list;
            }
        });

        List<List<Product>> collect = stringListJavaPairRDD.values().collect();
        System.out.println(collect);
    }

    public static void main(String[] args) throws Exception {
        new JavaSparkProduct().run(args);
    }

}
