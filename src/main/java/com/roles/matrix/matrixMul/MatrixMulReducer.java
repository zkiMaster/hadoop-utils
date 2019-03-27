package com.roles.matrix.matrixMul;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatrixMulReducer extends Reducer<Text,Text,Text,Text> {

    private  Text outKey=new Text();
    private  Text outValue=new Text();

    /**
     *
     * @param key  结果矩阵中的行号
     * @param values  列_值
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //只需要进行字符串拼接就好了
        StringBuilder sb=new StringBuilder();
        for (Text value:values) {
            sb.append(value+",");
        }
        String result=null;
        if (sb.toString().endsWith(",")) {
            result=sb.substring(0,sb.length()-1);
        }
        outKey.set(key);
        outValue.set(result);
        context.write(outKey,outValue);

    }
}
