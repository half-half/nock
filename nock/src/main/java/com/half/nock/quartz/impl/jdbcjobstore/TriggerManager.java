package com.half.nock.quartz.impl.jdbcjobstore;

/**
 * Created by yuhuijuan on 2018/11/21
 */

import org.quartz.TriggerKey;

/**
 *
 */
public class TriggerManager<T> {

    //consistent hash algorithm
    private final DistributeStrategy<T> strategy;


    private volatile String currentSemaphoreLock;

    public TriggerManager(DistributeStrategy<T> strategy) {
        this.strategy = strategy;
    }


    public String getCurrentSemaphoreLock() {
        return currentSemaphoreLock;
    }

    /**
     * update semaphore lock which belongs to this instance
     *
     * @param currentSemaphoreLock
     */
    public void setCurrentSemaphoreLock(String currentSemaphoreLock) {
        this.currentSemaphoreLock = currentSemaphoreLock;
    }

    /**
     * add instance node to hash circle
     * @param node
     */
    public synchronized void addInstanceNode(T node) {
        this.strategy.add(node);
    }

    /**
     * remove all instance nodes
     */
    public synchronized void reset() {

        this.strategy.clear();
    }

    /**
     * Judge if this trigger belongs to current instance
     *
     * @param key
     * @return
     */
    public boolean belongToCurrentNode(TriggerKey key) {
        return currentSemaphoreLock != null && strategy.get(key.getGroup() + "_" + key.getName()).equals(currentSemaphoreLock);
    }
}
