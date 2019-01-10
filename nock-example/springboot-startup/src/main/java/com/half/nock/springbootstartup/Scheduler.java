package com.half.nock.springbootstartup;

import com.half.nock.quartz.spring.quartz.SchedulerFactoryBean;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Scheduler {
    @Autowired
    private SchedulerFactoryBean schedulerFactoryBean;

    public Scheduler() {
        try {

            org.quartz.Scheduler scheduler = schedulerFactoryBean.getScheduler();

            scheduler.start();

        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }


}
