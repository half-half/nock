package com.half.nock.springbootstartup;

import com.half.nock.quartz.spring.quartz.SchedulerFactoryBean;
import org.quartz.Scheduler;

//@Component
public class SchedulerBean {

    private Scheduler scheduler;

    public SchedulerBean() {

    }

    public SchedulerBean(Scheduler scheduler) {
        this.scheduler = scheduler;
    }


}
