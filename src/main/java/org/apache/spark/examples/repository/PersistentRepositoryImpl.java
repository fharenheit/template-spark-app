package org.apache.spark.examples.repository;

/**
 * Persistence Object의 공통 CRUD를 제공하는 Repository의 구현체.
 */
public abstract class PersistentRepositoryImpl<D, P> extends DefaultSqlSessionDaoSupport implements PersistentRepository<D, P> {

    /**
     * MyBatis의 SQL Query를 실행하기 위한 SQLMap의 네임스페이스를 반환한다.
     * 일반적으로 이것의 이름은 Repository의 fully qualifed name을 사용한다.
     *
     * @return SQLMap의 네임스페이스
     */
    public abstract String getNamespace();

    @Override
    public int insert(D object) {
        return this.getSqlSessionTemplate().insert(this.getNamespace() + ".insert", object);
    }

    @Override
    public int update(D object) {
        return this.getSqlSessionTemplate().update(this.getNamespace() + ".update", object);
    }

    @Override
    public int delete(P identifier) {
        return this.getSqlSessionTemplate().delete(this.getNamespace() + ".delete", identifier);
    }

    @Override
    public D select(P identifier) {
        return this.getSqlSessionTemplate().selectOne(this.getNamespace() + ".select", identifier);
    }

    @Override
    public boolean exists(P identifier) {
        return (Integer) this.getSqlSessionTemplate().selectOne(this.getNamespace() + ".exist", identifier) > 0;
    }
}