package com.exem.bigdata.template.spark.util;

import org.apache.spark.sql.types.*;

public class DataTypeUtils {

    public static final DataType StringType = StringType$.MODULE$;
    public static final DataType BinaryType = BinaryType$.MODULE$;
    public static final DataType BooleanType = BooleanType$.MODULE$;
    public static final DataType DateType = DateType$.MODULE$;
    public static final DataType TimestampType = TimestampType$.MODULE$;
    public static final DataType CalendarIntervalType = CalendarIntervalType$.MODULE$;
    public static final DataType DoubleType = DoubleType$.MODULE$;
    public static final DataType FloatType = FloatType$.MODULE$;
    public static final DataType ByteType = ByteType$.MODULE$;
    public static final DataType IntegerType = IntegerType$.MODULE$;
    public static final DataType LongType = LongType$.MODULE$;
    public static final DataType ShortType = ShortType$.MODULE$;
    public static final DataType NullType = NullType$.MODULE$;

    public static DataType getDataType(String dataType) {
        switch (dataType.toUpperCase()) {
            case "STRING":
            case "VARCHAR":
                return StringType;
            case "BINARY":
            case "BLOB":
            case "CLOB":
                return BinaryType;
            case "BOOLEAN":
                return BooleanType;
            case "DATE":
                return DateType;
            case "TIMESTAMP":
                return TimestampType;
            case "CALENDAR":
                return CalendarIntervalType;
            case "DOUBLE":
                return DoubleType;
            case "FLOAT":
                return FloatType;
            case "BYTE":
            case "CHAR":
                return ByteType;
            case "INT":
            case "INTEGER":
                return IntegerType;
            case "LONG":
                return LongType;
            case "SHORT":
                return ShortType;
            case "":
            case "NULL":
                return NullType;
            default:
                return StringType;
        }
    }
}
