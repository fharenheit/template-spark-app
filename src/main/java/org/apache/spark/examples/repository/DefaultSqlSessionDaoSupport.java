package org.apache.spark.examples.repository;


import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.support.SqlSessionDaoSupport;
import org.springframework.util.Assert;

public class DefaultSqlSessionDaoSupport extends SqlSessionDaoSupport {

    public SqlSessionTemplate getSqlSessionTemplate() {
        Assert.isInstanceOf(SqlSessionTemplate.class, getSqlSession(), "SqlSessionTemplate isn't instance of SqlSession");
        return (SqlSessionTemplate) getSqlSession();
    }

}