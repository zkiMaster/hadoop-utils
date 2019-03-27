package com.roles.matrix.matrixT;

import com.utils.jobutils.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * 矩阵转置
 */
public class MatrixT {

    public static Job run(Configuration conf, String input, String output){
        JobUtil jobUtil=new JobUtil(conf);
        jobUtil.setMapper(MatrixTMapper.class, Text.class, Text.class)
                .setJarByClass(MatrixT.class)
                .setReduce(MatrixTReducer.class,Text.class,Text.class)
                .setInput(input)
                .setOutPut(output);
        return jobUtil.getJob();
    }


}
