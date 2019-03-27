package com.roles.matrix;

import com.utils.jobutils.JobListUtil;
import com.roles.matrix.matrixMul.MatrixMul;
import com.roles.matrix.matrixT.MatrixT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestTAndMul2 {

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

        JobListUtil jobListUtil=new JobListUtil();
        jobListUtil.addJob(job1);
        jobListUtil.addJob(job2);
        jobListUtil.run("TAndMul");

    }

}
