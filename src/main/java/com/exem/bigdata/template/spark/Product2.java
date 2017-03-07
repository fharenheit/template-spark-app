package com.exem.bigdata.template.spark;

import java.io.Serializable;

public class Product2 implements Serializable {
    String PRODUCT_CLASSIFICATION;
    String PRODUCT_NM;
    String BRAND_LINE;

    public String getPRODUCT_CLASSIFICATION() {
        return PRODUCT_CLASSIFICATION;
    }

    public void setPRODUCT_CLASSIFICATION(String PRODUCT_CLASSIFICATION) {
        this.PRODUCT_CLASSIFICATION = PRODUCT_CLASSIFICATION;
    }

    public String getPRODUCT_NM() {
        return PRODUCT_NM;
    }

    public void setPRODUCT_NM(String PRODUCT_NM) {
        this.PRODUCT_NM = PRODUCT_NM;
    }

    public String getBRAND_LINE() {
        return BRAND_LINE;
    }

    public void setBRAND_LINE(String BRAND_LINE) {
        this.BRAND_LINE = BRAND_LINE;
    }

    @Override
    public String toString() {
        return "Product1{" +
                "PRODUCT_CLASSIFICATION='" + PRODUCT_CLASSIFICATION + '\'' +
                ", PRODUCT_NM='" + PRODUCT_NM + '\'' +
                ", BRAND_LINE='" + BRAND_LINE + '\'' +
                '}';
    }
}
