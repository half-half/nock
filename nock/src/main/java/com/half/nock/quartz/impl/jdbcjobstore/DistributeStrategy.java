package com.half.nock.quartz.impl.jdbcjobstore;

/**
 * Created by yuhuijuan on 2018/11/20
 * 1、大概描述业务使用场景，非技术实现 <code> </code>  @see
 * 2、为什么有这个类或者接口
 * 3、接口哪些实现，有哪些方法大概说一下，特别说明内部默认的实现， 大概说一下默认实现的逻辑， <code> </code>  @see
 * 4、nock 在哪些地方使用，解决什么问题。 代码实现级别的。
 */
public interface DistributeStrategy<T> {

    /**
     * add node into hash circle
     *
     * @param node
     */
    void add(T node);

    /**
     * remove node from hash circle
     *
     * @param node
     */
    void remove(T node);

    /**
     * get matched node from hash circle
     *
     * @param key
     * @return
     */
    T get(Object key);

    /**
     * remove all nodes from hash cirle
     */
    void clear();
}
