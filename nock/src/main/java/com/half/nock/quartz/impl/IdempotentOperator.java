package com.half.nock.quartz.impl;

/**
 * Created by yuhuijuan on 2018/10/26
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * 幂等操作类
 *
 * @param <T>
 */

public class IdempotentOperator<T> {
    private static Logger logger = LoggerFactory.getLogger(IdempotentOperator.class);


    private Callable<T> task;

    public IdempotentOperator(Callable<T> task) {
        this.task = task;
    }

    public T execute() {
        return execute(10);
    }

    public T execute(int maxRetryTimes) {
        return execute(maxRetryTimes, 50L);
    }

    /**
     * 重试操作
     *
     * @param maxRetryTimes 重试次数
     * @return
     */
    public T execute(int maxRetryTimes, long sleepInterVal) {
        Throwable ex = null;
        boolean executeSuccess = false;
        T result = null;
        int retryTimes = 0;
        while (!executeSuccess && retryTimes++ < maxRetryTimes) {
            try {
                result = (T) task.call();
                executeSuccess = true;
            } catch (UnsupportedOperationException e) {
                logger.debug(e.getMessage());
                ex = e;
                try {
                    // logger.debug(Thread.currentThread().getName() + "   sleeping...for: " + (retryTimes % 5) * 10L);
                    Thread.sleep(sleepInterVal);
                    // logger.debug(Thread.currentThread().getName() + "   retry...for " + (retryTimes % 5) * 10L);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (Throwable e) {
                logger.warn(e.getMessage());
                ex = e;
                break;
            }
        }
        if (!executeSuccess)
            if (ex instanceof RuntimeException) {
                logger.error("超过重试次数,执行失败");
                throw (RuntimeException) ex;
            } else {
//				log.error(ex.getMessage());
                throw new RuntimeException("超过重试次数,执行失败");
            }
        // logger.info(Thread.currentThread().getId() + "   retry..." + retryTimes);

        return result;
    }
}