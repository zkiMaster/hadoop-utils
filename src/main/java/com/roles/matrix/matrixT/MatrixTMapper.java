package com.roles.matrix.matrixT;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixTMapper extends Mapper<LongWritable,Text,Text, Text> {

    private Text outKey=new Text();
    private Text outValue=new Text();

    /**
     *  1 1_0,2_3,3_-1,4_2,5_-3
     *  key: 1 行号
     *  value: 1_0 列_值
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、应该将右侧的矩阵转置
        String[] rowAndCol=value.toString().split(" ");
        String row=rowAndCol[0]; //行号
        System.out.println(value.toString());
        String[] cols=rowAndCol[1].split(",");

        //[1_0,2_3,3_-1,4_2,5_-3]
        for (int i=0;i<cols.length;i++) {
            String column=cols[i].split("_")[0]; //列号
            String valueStr=cols[i].split("_")[1]; //值

            //输出格式为： key: 列号   value：行号_值
            outKey.set(column);
            outValue.set(row+"_"+valueStr);

            context.write(outKey,outValue);
        }

    }
}
