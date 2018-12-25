package com.half.nock.quartz;

import com.half.nock.quartz.impl.StdSchedulerFactory;
import org.quartz.*;

/**
 * Created by yuhuijuan on 2018/10/8
 */
public class Test {
    public static void main(String[] args) {



        StdSchedulerFactory stdSchedulerFactory = null;
        try {
            stdSchedulerFactory = new StdSchedulerFactory("quartz.properties");

            Scheduler scheduler = stdSchedulerFactory.getScheduler();

            scheduler.start();


        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }
}
