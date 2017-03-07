package com.exem.bigdata.template.spark;

import com.exem.bigdata.template.spark.util.AbstractJob;
import com.exem.bigdata.template.spark.util.DateUtils;
import com.exem.bigdata.template.spark.util.SparkUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static com.exem.bigdata.template.spark.util.Constants.APP_FAIL;

public final class JavaSparkProduct2 extends AbstractJob {

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
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        // 스키마를 정의한다. 메타데이터 테이블에서 정보를 읽어와서 처리할 수 있다.
        StructType schema = new StructType(new StructField[]{
                new StructField("PRODUCT_CLASSIFICATION", DataTypes.StringType, true, Metadata.empty()),
                new StructField("PRODUCT_NM", DataTypes.StringType, true, Metadata.empty()),
                new StructField("BRAND_LINE", DataTypes.StringType, true, Metadata.empty())
        });

        // 인자가 1개 있는 UDF를 구현한다.
        UDF1 mode = new UDF1<String, String>() {
            public String call(final String value) throws Exception {
                return value.toUpperCase() + " 값";
            }
        };

        // UDF를 등록한다.
        sparkSession.udf().register("mode", mode, DataTypes.StringType);

        // 파일을 로딩한다.
        Dataset ds = sparkSession.read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(schema)
                .csv("product.txt");
/*
                .as(Encoders.bean(Product2.class));
*/

        System.out.println(ds.count());

        ds.printSchema();

        RelationalGroupedDataset grouped = ds.groupBy("BRAND_LINE");
        ds.agg(first("PRODUCT_CLASSIFICATION")).show();
//        grouped.agg(isnull(col("PRODUCT_CLASSIFICATION")).as("PRODUCT_CLASSIFICATION")).show();

        // errors.filter(col("line").like("%MySQL%")).count()

/*
        // UDF 함수를 사용한 Expression을 적용한다.
        Dataset dataset = ds.selectExpr("mode(BRAND_LINE)", "mode(PRODUCT_CLASSIFICATION)", "mode(PRODUCT_NM)");

        // 컬럼명을 변경한다
        Dataset brand_line = dataset.withColumnRenamed("UDF(BRAND_LINE)", "BRAND_LINE");
        brand_line.printSchema();
        brand_line.show();
        brand_line.selectExpr("first(BRAND_LINE)").show();
*/

    }

    public static void main(String[] args) throws Exception {
        new JavaSparkProduct2().run(args);
    }

}
