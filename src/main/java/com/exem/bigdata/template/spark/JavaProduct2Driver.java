package com.exem.bigdata.template.spark;

import com.exem.bigdata.template.spark.util.AbstractJob;
import com.exem.bigdata.template.spark.util.DataTypeUtils;
import com.exem.bigdata.template.spark.util.DateUtils;
import com.exem.bigdata.template.spark.util.SparkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

import static com.exem.bigdata.template.spark.util.Constants.APP_FAIL;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.first;

public final class JavaProduct2Driver extends AbstractJob {

    @Override
    protected SparkSession setup(String[] args) throws Exception {
        addOption("appName", "n", "Spark Application", "Spark Application (" + DateUtils.getCurrentDateTime() + ")");

        Map<String, String> argsMap = parseArguments(args);
        if (argsMap == null || argsMap.size() == 0) {
            System.exit(APP_FAIL);
        }
        return SparkUtils.getSparkSessionForLocal(getParamValue("appName"));
    }

    @Override
    protected void processing(SparkSession sparkSession) throws Exception {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        // 스키마를 정의한다. 메타데이터 테이블에서 정보를 읽어와서 처리할 수 있다.
        StructType schema = new StructType(new StructField[]{
            new StructField("PRODUCT_CLASSIFICATION", DataTypeUtils.getDataType("STRING"), true, Metadata.empty()),
            new StructField("PRODUCT_NM", DataTypes.StringType, true, Metadata.empty()),
            new StructField("BRAND_LINE", DataTypes.StringType, true, Metadata.empty()),
            new StructField("USE_YN", DataTypes.StringType, true, Metadata.empty())
        });

        // 인자가 1개 있는 UDF를 구현한다.
        UDF1 mode = new UDF1<String, String>() {
            public String call(final String value) throws Exception {
                return value.toUpperCase() + " 값";
            }
        };

        UDF1 isNullAndSpace = new UDF1<String, String>() {
            public String call(final String value) throws Exception {
                return StringUtils.isEmpty(value) ? " " : value;
            }
        };

        // 인자가 2개 있는 UDF를 구현한다.
        UDF2 isNotAllowChar = new UDF2<String, String, Boolean>() {
            @Override
            public Boolean call(String srcValue, String checkChar) throws Exception {
                String[] arrCheckChar = StringUtils.split(checkChar, ";");
                boolean isAllowChar = true;
                for (int i = 0; arrCheckChar != null && i < arrCheckChar.length; i++) {
                    isAllowChar = isAllowChar && StringUtils.equals(srcValue, arrCheckChar[i]);
                }
                return !isAllowChar;
            }
        };

        // UDF를 등록한다.
        sparkSession.udf().register("mode", mode, DataTypes.StringType);
        sparkSession.udf().register("isNotAllowChar", isNotAllowChar, DataTypes.BooleanType);
        sparkSession.udf().register("isNull", isNullAndSpace, DataTypes.StringType);

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

        long total = ds.filter("USE_YN = 'Y'").count();

        RelationalGroupedDataset grouped = ds.groupBy("PRODUCT_CLASSIFICATION");

        Dataset<Row> count = grouped.count();
        Dataset<Row> agg = grouped.agg(
            count("*").as("COUNT"),
            first("PRODUCT_NM").as("PRODUCT_NM"),
            first("BRAND_LINE").as("BRAND_LINE")
        ).filter("COUNT > 1");

//        count.join(agg).show();
//        ds.filter("count >= 3");


        agg.show();

//        Dataset<Row> selected = agg.selectExpr("isNull(PRODUCT_CLASSIFICATION)", "isNull(PRODUCT_NM)", "isNull(BRAND_LINE)");


//        SELECT PRODUCT_ID_CD
//        FROM DP_PRODUCTS
//        WHERE USE_YN=‘Y’


//        ds.filter("count >= 3").show();
//        ds.groupBy("PRODUCT_NM").agg(count("*").alias("cnt")).where("cnt > 1").show();
//        ds.groupBy("PRODUCT_NM").agg(count("*").alias("cnt")).where("cnt > 1").show();


//        ds.agg(first("PRODUCT_CLASSIFICATION")).filter("").show();
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
        new JavaProduct2Driver().run(args);
    }

}
