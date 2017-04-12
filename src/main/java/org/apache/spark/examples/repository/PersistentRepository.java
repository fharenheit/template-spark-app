package org.apache.spark.examples.repository;


/**
 * Persistence Object의 공통 CRUD를 정의한 Repository.
 */
public interface PersistentRepository<D, P> {

    /**
     * 새로운 객체를 저장한다.
     *
     * @param object 저장할 객체
     * @return 저장한 건수
     */
    int insert(D object);

    /**
     * 지정한 객체의 정보를 업데이트한다.
     *
     * @param object 업데이트할 객체
     * @return 업데이트 건수
     */
    int update(D object);

    /**
     * 지정한 식별자에 해당하는 객체를 삭제한다.
     *
     * @param identifier 식별자
     * @return 삭제한 건수
     */
    int delete(P identifier);

    /**
     * 지정한 식별자에 해당하는 객체를 조회한다.
     *
     * @param identifier 식별자
     * @return 식별자로 식별하는 객체
     */
    D select(P identifier);

    /**
     * 지정한 식별자에 해당하는 객체가 존재하는지 확인한다.
     *
     * @param identifier 식별자
     * @return 존재하는 경우 <tt>true</tt>
     */
    boolean exists(P identifier);
}