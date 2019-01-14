package com.half.nock.jobbean;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobBean implements Job {

    private static Logger logger = LoggerFactory.getLogger(JobBean.class);

    public JobBean() {
        super();
    }


    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        System.out.println(context.getTrigger().getKey() + "fire ~");
    }
}