package org.apache.spark.examples.repository;

import org.mybatis.spring.SqlSessionTemplate;

import java.util.List;
import java.util.Map;

public class DummyRepository extends PersistentRepositoryImpl {

    public static final String NAMESPACE = DummyRepository.class.getName();

    public String getNamespace() {
        return this.NAMESPACE;
    }

    public DummyRepository(SqlSessionTemplate template) {
        super.setSqlSessionTemplate(template);
    }

    public List<Map> select() {
        return this.getSqlSessionTemplate().selectList(this.getNamespace() + ".select");
    }

}
