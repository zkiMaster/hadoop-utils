package com.roles.matrix.matrixMul;

import com.utils.jobutils.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.net.URI;

/**
 * 矩阵乘法
 */
public class MatrixMul {

    /**
     * @param conf
     * @param input 输入路径
     * @param output 输出路径
     * @param cachepath 右边的缓存矩阵
     * @return
     */
    public static Job run(Configuration conf,String input,String output,URI cachepath){
        JobUtil jobUtil=new JobUtil(conf);
        //将第一步的输出作为全局缓存
        jobUtil.getJob().addCacheFile(cachepath);
        jobUtil.setMapper(MatrixMulMapper.class, Text.class, Text.class)
                .setJarByClass(MatrixMul.class)
                .setReduce(MatrixMulReducer.class, Text.class, Text.class)
                .setInput(input)
                .setOutPut(output);
        return jobUtil.getJob();
    }
}
