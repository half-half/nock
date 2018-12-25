package com.half.nock.quartz.impl.jdbcjobstore;

/**
 * Created by yuhuijuan on 2018/10/11
 */
public class TriggerSemaphore implements Comparable<TriggerSemaphore> {

    private String name;
    private String instanceName;
    private Long checkTime;

    /**
     * 信号量的名称
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * 信号量的名称
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 被抢占的scheduler名称
     *
     * @return
     */
    public String getInstanceName() {
        return instanceName;
    }

    /**
     * 被抢占的scheduler名称
     *
     * @param instanceName
     */
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    /**
     * 上次被抢占时间
     *
     * @return
     */
    public Long getCheckTime() {
        return checkTime;
    }

    /**
     * 上次被抢占时间
     *
     * @param checkTime
     */
    public void setCheckTime(Long checkTime) {
        this.checkTime = checkTime;
    }

    @Override
    public int compareTo(TriggerSemaphore o) {

        return this.name.equals(o.name) ? 1 : 0;
    }
}