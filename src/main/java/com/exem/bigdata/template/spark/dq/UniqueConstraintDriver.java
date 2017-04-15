package com.exem.bigdata.template.spark.dq;

import com.exem.bigdata.template.spark.util.AbstractJob;
import com.exem.bigdata.template.spark.util.DataTypeUtils;
import com.exem.bigdata.template.spark.util.DateUtils;
import com.exem.bigdata.template.spark.util.SparkUtils;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.convert.Wrappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.exem.bigdata.template.spark.util.Constants.APP_FAIL;


public class UniqueConstraintDriver extends AbstractJob {

    public static Logger logger = LogManager.getLogger(UniqueConstraintDriver.class);

    @Override
    protected SparkSession setup(String[] args) throws Exception {

        addOption("appName", "n", "Unique Constraint DQ Job", "Unique Constraint DQ Job (" + DateUtils.getCurrentDateTime() + ")");

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

        // Unique Constraints
        String[] constraints = {"PRODUCT_CLASSIFICATION", "USE_YN"};

        // 파일을 로딩한다.
        Dataset ds = sparkSession.read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(schema)
                .csv("product.txt");

        JavaRDD rdd = ds.toJavaRDD();

        List list = uniqueConstraint(rdd, constraints);

        System.out.println(list);
    }

    private List uniqueConstraint(JavaRDD rdd, String... columns) {
        List result = new ArrayList();
        for (String column : columns) {
            // Group By (유일성 컬럼을 모두 반복 수행해야 함)
            JavaPairRDD pairRDD = rdd.groupBy(new Function<GenericRowWithSchema, Object>() {
                @Override
                public Object call(GenericRowWithSchema row) throws Exception {
                    int index = row.fieldIndex(column);
                    return row.get(index); // Group By시 사용할 컬럼값
                }
            });

            // Group By한 결과를 처리
            JavaRDD mappedRDD = pairRDD.flatMap(new FlatMapFunction<Tuple2, List>() {
                @Override
                public Iterator call(Tuple2 t) throws Exception {
                    Wrappers.IterableWrapper iterable = (Wrappers.IterableWrapper) t._2();
                    // 개수가 1개가 넘는다면 유일성 위배
                    if (iterable.size() > 1) {
                        // 여기에서 결과를 포맷팅한다. 향후 해야할 작업.
                        return new ArrayIterator(iterable.toArray());
                    }
                    // 그외의 경우 모두 NULL 처리
                    return null;
                }
            });


            // 1개가 넘는 데이터만 모두 취합
            result.addAll(mappedRDD.collect());

            mappedRDD.unpersist();
            pairRDD.unpersist();
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        new UniqueConstraintDriver().run(args);
    }

}
