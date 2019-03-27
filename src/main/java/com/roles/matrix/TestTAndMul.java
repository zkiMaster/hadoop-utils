package com.roles.matrix;

import com.roles.matrix.matrixMul.MatrixMul;
import com.roles.matrix.matrixT.MatrixT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestTAndMul {

    private static String step1_input = "matrix/input/step1_input/matrix1.txt";
    private static String step1_output = "matrix/output/step1_output";
    private static String step2_input = "matrix/input/step2_input/matrix2.txt";
    private static String step2_output = "matrix/output/step2_output";

    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://os01:9000");
        Job job1 = MatrixT.run(conf, step1_input, step1_output);
        //将第一步的输出作为全局缓存
        URI cache = new URI(step1_output + "/part-r-00000");
        Job job2= MatrixMul.run(conf,step2_input,step2_output,cache);

        ControlledJob ctrljob1 = new ControlledJob(conf);
        ctrljob1.setJob(job1);

        ControlledJob ctrljob2 = new ControlledJob(conf);
        ctrljob2.setJob(job2);
        ctrljob2.addDependingJob(ctrljob1);

        // 主控制器
        JobControl jobCtrl = new JobControl("myctrl");
        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);

        // 在启动线程，记住一定要有这个
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {
            // 如果作业全部完成，就打印成功作业的信息
            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
    }

}
