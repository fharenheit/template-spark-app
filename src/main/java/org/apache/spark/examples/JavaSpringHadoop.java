package org.apache.spark.examples;

import org.apache.spark.examples.repository.DummyRepository;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;
import java.util.Map;

public class JavaSpringHadoop {

    public static void main(String[] args) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:/hadoop/applicationContext-mini.xml");
    }

}
