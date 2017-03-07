Spark Application Template 소개
------------

본 프로젝트는 Spark Application 개발을 위한 템플릿 프로젝트입니다. IntelliJ IDEA에서 Maven Import를 한 후 직접 IntelliJ IDEA에서 실행할 수 있도록 구성되어 있습니다.

컴파일
------------

소스코드를 컴파일 하기 위해서는 Apache Maven이 필요하며 IntelliJ IDEA를 이용하는 경우는 Maven Import를 하도록 합니다.
커맨드라인에서 컴파일을 하기 위해서는 Apache Maven을 설정한 후에 다음의 커맨드를 실행합니다. 

~~~
# mvn clean package
~~~

Custom Data Type
------------

~~~
public class University implements Serializable {
    private String name;
    private long numStudents;
    private long yearFounded;

    public void setName(String name) {...}
    public String getName() {...}
    public void setNumStudents(long numStudents) {...}
    public long getNumStudents() {...}
    public void setYearFounded(long yearFounded) {...}
    public long getYearFounded() {...}
}

class BuildString implements MapFunction {
    public String call(University u) throws Exception {
        return u.getName() + " is " + (2015 - u.getYearFounded()) + " years old.";
    }
}

Dataset schools = context.read().json("/schools.json").as(Encoders.bean(University.class));
Dataset strings = schools.map(new BuildString(), Encoders.STRING());
~~~