package com.half.nock.quartz.impl.jdbcjobstore;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created by yuhuijuan on 2018/10/12
 */
/*
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 *
 * Consistant Hash
 *
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */
public class ConsistentHashDistribution<T> implements DistributeStrategy<T> {

    private HashSupport hashSupport;
    private final int numberOfReplicas = 100;// 节点的复制因子,实际节点个数 * numberOfReplicas =
    // 虚拟节点个数
    private final SortedMap<Long, T> circle = new TreeMap<>();// 存储虚拟节点的hash值到真实节点的映射

    public ConsistentHashDistribution() {
        this.hashSupport = new HashSupport() {

            @Override
            public long compute(String key) {
                MessageDigest md5 = null;
                try {
                    md5 = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalStateException("no md5 algrithm found");
                }

                md5.reset();
                md5.update(key.getBytes());
                byte[] bKey = md5.digest();
                //具体的哈希函数实现细节--每个字节 & 0xFF 再移位
                long result = ((long) (bKey[3] & 0xFF) << 24)
                        | ((long) (bKey[2] & 0xFF) << 16
                        | ((long) (bKey[1] & 0xFF) << 8) | (long) (bKey[0] & 0xFF));
                return result & 0xffffffffL;
            }
        };
    }

    public ConsistentHashDistribution(HashSupport hashSupport) {
        this.hashSupport = hashSupport;
    }

    public ConsistentHashDistribution(Collection<T> nodes) {
        for (T node : nodes)
            add(node);
    }

    @Override
    public void add(T node) {
        for (int i = 0; i < numberOfReplicas; i++)
            // 对于一个实际机器节点 node, 对应 numberOfReplicas 个虚拟节点
            /*
             * 不同的虚拟节点(i不同)有不同的hash值,但都对应同一个实际机器node
             * 虚拟node一般是均衡分布在环上的,数据存储在顺时针方向的虚拟node上
             */
            circle.putIfAbsent(hashSupport.compute(node.toString() + i), node);
    }

    @Override
    public void remove(T node) {
        for (int i = 0; i < numberOfReplicas; i++)
            circle.remove(hashSupport.compute(node.toString() + i));
    }

    /*
     * 获得一个最近的顺时针节点,根据给定的key 取Hash
     * 然后再取得顺时针方向上最近的一个虚拟节点对应的实际节点
     * 再从实际节点中取得 数据
     */
    @Override
    public T get(Object key) {
        if (circle.isEmpty())
            return null;
        long hash = hashSupport.compute((String) key);// node 用String来表示,获得node在哈希环中的hashCode
        if (!circle.containsKey(hash)) {//数据映射在两台虚拟机器所在环之间,就需要按顺时针方向寻找机器
            SortedMap<Long, T> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    public long getSize() {
        return circle.size();
    }

    /**
     * clear hash circle
     */
    public void clear() {
        circle.clear();
    }

    /*
     * 查看MD5算法生成的hashCode值---表示整个哈希环中各个虚拟节点位置
     */
    public void testBalance() {
        Set<Long> sets = circle.keySet();//获得TreeMap中所有的Key
        SortedSet<Long> sortedSets = new TreeSet<Long>(sets);//将获得的Key集合排序
        Long lastCode = Long.valueOf(0);
        for (Long hashCode : sortedSets) {

            System.out.println(hashCode + "    :  " + (hashCode - lastCode));
            lastCode = hashCode;
        }

        System.out.println("----each location 's distance are follows: ----");
        /*
         * 查看用MD5算法生成的long hashCode 相邻两个hashCode的差值
         */
        Iterator<Long> it = sortedSets.iterator();
        Iterator<Long> it2 = sortedSets.iterator();
        if (it2.hasNext())
            it2.next();
        long keyPre, keyAfter;
        while (it.hasNext() && it2.hasNext()) {
            keyPre = it.next();
            keyAfter = it2.next();
            System.out.println(keyAfter - keyPre);
        }
    }

    static interface HashSupport {
        /**
         * 实现hash的算法
         *
         * @param key
         * @return
         */
        public long compute(String key);
    }
}