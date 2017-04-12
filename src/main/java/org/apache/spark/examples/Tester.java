package org.apache.spark.examples;

import org.apache.spark.examples.repository.DummyRepository;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;
import java.util.Map;

public class Tester {

    public static void main(String[] args) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:/spring/*.xml");
        DummyRepository repo = ctx.getBean(DummyRepository.class);
        List<Map> selected = repo.select();
        System.out.println(selected);
    }

}
