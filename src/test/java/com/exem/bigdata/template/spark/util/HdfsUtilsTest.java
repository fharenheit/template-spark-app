package com.exem.bigdata.template.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;

import java.io.IOException;

public class HdfsUtilsTest {

    @Test
    public void getConfigurationFromClasspath() throws IOException {
        Configuration conf = HdfsUtils.getConfiguration("classpath:com/exem/bigdata/template/spark/util/core-default.xml");
        Assert.assertEquals("0.23.0", conf.get("hadoop.common.configuration.version"));
    }

    @Test
    public void getConfigurationFromPath() throws IOException {
        Configuration conf = HdfsUtils.getConfiguration("file:target/test-classes/com/exem/bigdata/template/spark/util/core-default.xml");
        Assert.assertEquals("0.23.0", conf.get("hadoop.common.configuration.version"));
    }

    @Test
    public void getHdfsResourceLoader() throws IOException {
        Configuration conf = HdfsUtils.getConfiguration("classpath:com/exem/bigdata/template/spark/util/core-default.xml");
        HdfsResourceLoader loader = new HdfsResourceLoader(conf);
        FileSystem fs = loader.getFileSystem();
        Assert.assertEquals("org.apache.hadoop.fs.LocalFileSystem", fs.getClass().getName());
    }
}
