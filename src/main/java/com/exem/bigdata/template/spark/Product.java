package com.exem.bigdata.template.spark;

import java.io.Serializable;

public class Product implements Serializable {
    String GROUPED_KEY;
    String PRODUCT_CLASSIFICATION;
    String PRODUCT_NM;
    String BRAND_LINE;

    @Override
    public String toString() {
        return "Product{" +
                "GROUPED_KEY='" + GROUPED_KEY + '\'' +
                ", PRODUCT_CLASSIFICATION='" + PRODUCT_CLASSIFICATION + '\'' +
                ", PRODUCT_NM='" + PRODUCT_NM + '\'' +
                ", BRAND_LINE='" + BRAND_LINE + '\'' +
                '}';
    }
}
