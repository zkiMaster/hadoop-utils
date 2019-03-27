package com.utils.jobutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
import java.util.List;

public class JobListUtil {
    private static ControlledJob[] ctrlJobList = new ControlledJob[10];
    private static Integer size = 0;
    private static JobControl jobControl = null;

    public JobListUtil addJob(Job job) {
        try {
            //每次都先检测数组长度
            addCapacity();
            Configuration conf = job.getConfiguration();
            ControlledJob controlledJob = new ControlledJob(conf);
            controlledJob.setJob(job);
            ctrlJobList[size] = controlledJob;
            size++;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    public JobListUtil addJob(List<Job> jobList) {
        for (Job job : jobList) {
            addJob(job);
        }
        return this;
    }

    private void addCapacity() {
        if (size >= ctrlJobList.length * 3 / 4) {
            ControlledJob[] newCtrlJobList = new ControlledJob[ctrlJobList.length * 2];
            for (int i = 0; i < size; i++) {
                newCtrlJobList[i] = ctrlJobList[i];
            }
            ctrlJobList = newCtrlJobList;
        }
    }

    public void run(String jobName) {
        jobControl = new JobControl(jobName);
        System.out.println(size);
        //设置依赖关系
        for (int i = size - 1; i > 0; i--) {
            ctrlJobList[i].addDependingJob(ctrlJobList[i - 1]);
            jobControl.addJob(ctrlJobList[size - i - 1]);
        }
        jobControl.addJob(ctrlJobList[size - 1]);

        //启动线程
        Thread t = new Thread(jobControl);
        t.start();
            while (true) {
            // 如果作业全部完成，就打印成功作业的信息
            if (jobControl.allFinished()) {
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                break;
            }
        }
    }
}
