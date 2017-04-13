## Spark Application Template 소개

본 프로젝트는 Spark Application 개발을 위한 템플릿 프로젝트입니다. IntelliJ IDEA에서 Maven Import를 한 후 직접 IntelliJ IDEA에서 실행할 수 있도록 구성되어 있습니다.

### 컴파일

소스코드를 컴파일 하기 위해서는 Apache Maven이 필요하며 IntelliJ IDEA를 이용하는 경우는 Maven Import를 하도록 합니다.
커맨드라인에서 컴파일을 하기 위해서는 Apache Maven을 설정한 후에 다음의 커맨드를 실행합니다. 

```bash
# mvn clean package
```

### 간단한 Spark Application 작성하기

본 예제는 Spark Job을 실행할 때 커맨드 라인 옵션을 지정할 수 있는 기능을 제공하는 `AbstractJob` 클래스를 제공합니다. 따라서 이러한 기능을 사용하기 위해서는 다음과 같이 Spark Driver 개발시 아래와 같이 상속하도록 합니다.

```java
import com.exem.bigdata.template.spark.util.AbstractJob;
import com.exem.bigdata.template.spark.util.DateUtils;
import com.exem.bigdata.template.spark.util.SparkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.exem.bigdata.template.spark.util.Constants.APP_FAIL;

public final class JavaSparkDriver extends AbstractJob {

    @Override
    protected SparkSession setup(String[] args) throws Exception {
        // 커맨드 라인 옵션을 추가합니다. (--appName 또는 -n)
        addOption("appName", "n", "Spark Application", "Spark Application (" + DateUtils.getCurrentDateTime() + ")");

        // 사용자가 입력한 커맨드 라인을 Map으로 꺼내옵니다. 
        Map<String, String> argsMap = parseArguments(args);
        if (argsMap == null || argsMap.size() == 0) {
            System.exit(APP_FAIL);
        }
        
        // 커맨드 라인으로 입력한 --appName 파라미터로 Spark Session을 생성합니다.
        return SparkUtils.getSparkSessionForLocal(getParamValue("appName")); // Local Spark Job
    }

    @Override
    protected void processing(SparkSession sparkSession) throws Exception {
        // 실제 작업을 실행하기 위해서 Spark Context를 생성하고 텍스트 파일을 로딩하여 RDD를 생성합니다.
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> stringRDD = jsc.textFile("product.txt").persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println(stringRDD.count());

        ... 생략
    }

    @Override
    protected void cleanup(SparkSession session) throws Exception {
        // 기타 setup() 메소드 등에서 open한 지원을 Close합니다. Spark Session은 AbstractJob에서 자동으로 Close합니다.
        
        ... 생략
    }

    public static void main(String[] args) throws Exception {
        new JavaSparkDriver().run(args);
    }

}

```

### Spring Data Hadoop 지원

Spring Data Hadoop을 통해 몇 가지 작업을 간편하게 사용할 수 있으므로 Maven POM인 `pom.xml` 파일에 다음을 추가합니다.

```xml
<!-- =============== -->
<!--  Spring Hadoop  -->
<!-- =============== -->

<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-data-hadoop</artifactId>
    <version>${spring.hadoop.version}</version>
</dependency>
<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-data-hadoop-batch</artifactId>
    <version>${spring.hadoop.version}</version>
</dependency>
<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-data-hadoop-test</artifactId>
    <version>${spring.hadoop.version}</version>
</dependency>
<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-yarn-test</artifactId>
    <version>${spring.hadoop.version}</version>
</dependency>
```

Hadoop의 주요 설정 파일인 `*-site.xml` 파일의 위치는 어디에 있어도 상관없으나 본 예제에서는 `/src/main/resources/hadoop/core-site.xml` 파일을 사용하도록 하였습니다.

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>

    <property>
        <name>hadoop.common.configuration.version</name>
        <value>0.23.0</value>
        <description>version of this configuration file</description>
    </property>

    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop-${user.name}</value>
        <description>A base for other temporary directories.</description>
    </property>

    <property>
        <name>io.native.lib.available</name>
        <value>true</value>
        <description>Controls whether to use native libraries for bz2 and zlib
            compression codecs or not. The property does not control any other native
            libraries.
        </description>
    </property>

</configuration>
```

Spring Data Hadoop을 이용하여 Hadoop Configuration 및 FileSystem을 초기화 하기 위해서 다음과 같이 Spring XML 파일(`/src/main/resources/hadoop/applicationContext-hadoop.xml`)을 작성할 수 있습니다.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:hdp="http://www.springframework.org/schema/hadoop"
       xmlns:context="http://www.springframework.org/schema/context"
       xsi:schemaLocation="http://www.springframework.org/schema/beans  http://www.springframework.org/schema/beans/spring-beans.xsd http://www.springframework.org/schema/hadoop http://www.springframework.org/schema/hadoop/spring-hadoop.xsd http://www.springframework.org/schema/context http://www.springframework.org/schema/context/spring-context.xsd">

    <hdp:configuration id="conf" resources="classpath:/hadoop/core-site.xml"/>

    <hdp:resource-loader id="loader" configuration-ref="conf"/>

    <hdp:file-system id="fs" uri="hdfs://localhost:8080" configuration-ref="conf"/>

</beans>

```

통합 테스트를 위해서 YARN 기반 Mini Cluster를 생성하기 위해서 다음과 같이 Spring XML 파일(`/src/main/resources/hadoop/applicationContext-mini.xml`)을 작성할 수 있습니다.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:hdp="http://www.springframework.org/schema/hadoop"
       xmlns:context="http://www.springframework.org/schema/context"
       xsi:schemaLocation="http://www.springframework.org/schema/beans  http://www.springframework.org/schema/beans/spring-beans.xsd http://www.springframework.org/schema/hadoop http://www.springframework.org/schema/hadoop/spring-hadoop.xsd http://www.springframework.org/schema/context http://www.springframework.org/schema/context/spring-context.xsd">

    <hdp:configuration id="conf" resources="classpath:/hadoop/core-site.xml"/>

    <hdp:resource-loader id="loader" configuration-ref="conf"/>

    <hdp:file-system id="fs" uri="hdfs://localhost:8080" configuration-ref="conf"/>

    <bean id="cluster" class="org.springframework.yarn.test.support.YarnClusterFactoryBean">
        <property name="clusterId" value="YarnClusterTests"/>
        <property name="autoStart" value="true"/>
        <property name="nodes" value="1"/>
    </bean>
</beans>
```


### Spring Framework + MyBATIS 지원

Spark Application이 실행하는데 있어서 JDBC 연결이 필요한 경우 MyBATIS를 사용해야할 수 있습니다. 본 예제에는 Spring + MyBATIS를 포함한 예제를 담고 있습니다.

* Spring Application Context XML
  * DataSource 등을 초기화하는 Spring XML - `/src/main/resourcs/spring/applicationContext.xml`
  * MyBATIS 기반 Repository - `/src/main/resourcs/spring/applicationContext-repository.xml`
* 기타 설정 파일
  * 환경설정 파일 - `/src/main/resourcs/config.properties`
  * DDL 파일 - `/src/main/resourcs/import.sql`

#### Maven Dependencies

Spring Framework 및 MyBATIS를 사용하기 위해서 Maven POM인 `pom.xml` 파일에 다음을 추가합니다.

```xml
<!-- =============== -->
<!--  Spring Hadoop  -->
<!-- =============== -->

<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-data-hadoop</artifactId>
    <version>${spring.hadoop.version}</version>
</dependency>
<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-data-hadoop-batch</artifactId>
    <version>${spring.hadoop.version}</version>
</dependency>
<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-data-hadoop-test</artifactId>
    <version>${spring.hadoop.version}</version>
</dependency>
<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-yarn-test</artifactId>
    <version>${spring.hadoop.version}</version>
</dependency>

<!-- ====================== -->
<!--  MyBatis Dependencies  -->
<!-- ====================== -->

<dependency>
    <groupId>org.mybatis</groupId>
    <artifactId>mybatis</artifactId>
    <version>${mybatis.version}</version>
</dependency>
<dependency>
    <groupId>org.mybatis</groupId>
    <artifactId>mybatis-spring</artifactId>
    <version>${mybatis.spring.version}</version>
    <exclusions>
        <exclusion>
            <groupId>org.springframework</groupId>
            <artifactId>spring-tx</artifactId>
        </exclusion>
        <exclusion>
            <groupId>org.springframework</groupId>
            <artifactId>spring-jdbc</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

#### 환경설정 파일

`/src/main/resourcs/config.properties` 파일에는 커넥션 풀 등의 설정 정보를 포함하고 있습니다.

```properties
###########################################
## JDBC Configuration
###########################################

# PostgreSQL

#jdbc.driver=org.postgresql.Driver
#jdbc.url=jdbc:postgresql://localhost:5432/test
#jdbc.username=postgres
#jdbc.password=postgres
#jdbc.min.pool=3
#jdbc.max.pool=10

# MySQL

jdbc.driver=com.mysql.cj.jdbc.Driver
jdbc.url=jdbc:mysql://localhost/test?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=Asia/Seoul
jdbc.username=root
jdbc.password=root
jdbc.max.pool=10
```

#### Spring Framework Application Context XML 파일

`/src/main/resourcs/spring/applicationContext.xml` 파일에는 다음과 같이 커넥션 풀 및 MyBATIS Configuration 설정을 포함하고 있습니다.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://www.springframework.org/schema/beans  http://www.springframework.org/schema/beans/spring-beans.xsd">

    <!-- ========================= PROPERTY PLACE HOLDER DEFINITION ========================= -->

    <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">
        <property name="locations">
            <list>
                <value>classpath:config.properties</value>
            </list>
        </property>
    </bean>

    <!-- ========================= TRANSLATION DEFINITION ========================= -->

    <bean class="org.mybatis.spring.MyBatisExceptionTranslator">
        <constructor-arg ref="dataSource"/>
        <constructor-arg value="false"/>
    </bean>

    <bean id="jdbcTemplate" class="org.springframework.jdbc.core.JdbcTemplate">
        <property name="dataSource" ref="dataSource"/>
    </bean>

    <!-- ========================= MYBATIS DEFINITION ========================= -->

    <bean id="sqlSessionFactory" class="org.mybatis.spring.SqlSessionFactoryBean">
        <property name="dataSource" ref="dataSource"/>
        <property name="configLocation" value="classpath:/mybatis/mybatis-config.xml"/>
        <property name="mapperLocations" value="classpath:/mybatis/*-mapper.xml"/>
    </bean>

    <bean id="sqlSessionTemplate" class="org.mybatis.spring.SqlSessionTemplate">
        <constructor-arg ref="sqlSessionFactory"/>
    </bean>

    <!-- ========================= TRANSACTION DEFINITION ========================= -->

    <bean id="transactionManager" class="org.springframework.jdbc.datasource.DataSourceTransactionManager">
        <property name="dataSource" ref="dataSource"/>
    </bean>

    <!-- ========================= DATASOURCE DEFINITION ========================= -->

    <bean id="dataSource" class="org.apache.ibatis.datasource.pooled.PooledDataSource">
        <property name="password" value="${jdbc.password}"/>
        <property name="username" value="${jdbc.username}"/>
        <property name="driver" value="${jdbc.driver}"/>
        <property name="url" value="${jdbc.url}"/>
        <property name="poolMaximumActiveConnections" value="${jdbc.max.pool}"/>
    </bean>

</beans>
```
#### MyBATIS Configuration 파일

`/src/main/resources/mybatis/mybatis-config.xml` 파일은 다음과 같이 작성하며 기본적인 MyBATIS 설정 정보를 포함합니다. 이 파일은 Spring Framework에서 로딩합니다.

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE configuration PUBLIC "-//mybatis.org//DTD Config 3.0//EN" "http://mybatis.org/dtd/mybatis-3-config.dtd">

<configuration>

    <settings>
        <setting name="cacheEnabled" value="false"/>
        <setting name="useGeneratedKeys" value="true"/>
        <setting name="defaultExecutorType" value="REUSE"/>
        <setting name="autoMappingBehavior" value="PARTIAL"/>
    </settings>

</configuration>
```

#### Repository 선언

`/src/main/resourcs/spring/applicationContext-repository.xml` 파일에는 MyBATIS 기반으로 동작하는 Repository 클래스를 정의합니다. 

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://www.springframework.org/schema/beans  http://www.springframework.org/schema/beans/spring-beans.xsd">

    <bean name="dummyRepository" class="org.apache.spark.examples.repository.DummyRepository">
        <constructor-arg name="template" ref="sqlSessionTemplate"/>
    </bean>

</beans>
```

#### MyBATIS Mapper XML 정의

`/src/main/resources/mybatis/dummy-mapper.xml` 파일은 MyBATIS Mapper XML 파일입니다. `namespace`는 반드시 Repository 클래스의 fully qualified class name으로 정의해야 합니다.

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="org.apache.spark.examples.repository.DummyRepository">

    <select id="select" resultType="map">
        SELECT * FROM TEST
    </select>

</mapper>
```

#### Repository 클래스 구현

Repository를 구현하기 위해서는  `PersistentRepositoryImpl` 클래스를 상속받고 다음과 같이 `NAMESPACE`를 정의한 후 생성자를 통해 `SqlSessionTemplate`을 받도록 합니다. 그리고 다음과 같이 `select()` 메소드를 구현한 후 구현시 MyBATIS Mapper XML에 정의되어 있는 `select` 쿼리를 호출합니다.

```java
import org.mybatis.spring.SqlSessionTemplate;

import java.util.List;
import java.util.Map;

public class DummyRepository extends PersistentRepositoryImpl {

    public static final String NAMESPACE = DummyRepository.class.getName(); // 반드시 정의합니다.

    public String getNamespace() { // 반드시 정의합니다.
        return this.NAMESPACE;
    }

    public DummyRepository(SqlSessionTemplate template) { // MyBATIS를 사용하기 위해서 Spring에서 Constructor Injection을 하기 위해서 반드시 정의합니다.
        super.setSqlSessionTemplate(template);
    }

    public List<Map> select() {
        // /src/main/resources/mybatis/dummy-mapper.xml 파일에 정의한 select 쿼리를 호출합니다.
        return this.getSqlSessionTemplate().selectList(this.getNamespace() + ".select");
    }

}
```

#### Spring 초기화 및 Repository 호출하기

다음의 코드를 통해 Spring Framework를 초기화 하고 MyBATIS로 동작하는 Repository를 호출할 수 있습니다.

```java
// Spring Framework를 초기화 합니다.
ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:/spring/*.xml");

// 데이터베이스 쿼리를 실행하기 위해서 MyBATIS로 구현한 Repository를 Spring IoC Container에서 꺼냅니다.
DummyRepository repo = ctx.getBean(DummyRepository.class);

// Select 쿼리를 호출합니다.
List<Map> selected = repo.select();
```

### 기타

#### Hadoop 배포판의 Spark 버전 제약

* Cloudera CDH 최신 버전인 5.10 버전에는 여전에 Spark 1.6 버전이 사용되고 있습니다.
* Hortonworks HDP 최신 버전에는 Spark 1.6과 Spark 2.1 버전이 같이 포함되어 있습니다.
