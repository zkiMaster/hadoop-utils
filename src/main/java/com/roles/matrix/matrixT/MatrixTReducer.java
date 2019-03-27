package com.roles.matrix.matrixT;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatrixTReducer extends Reducer<Text,Text,Text,Text> {

    private Text outKey=new Text();
    private Text outValue=new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //输入格式为： key: 列号   value：行号_值
        StringBuilder sb=new StringBuilder();
        for (Text t:values) {
            //主要是将输入的结果拼接为字符串就可以了
            sb.append(t+",");
        }
        String line=null;
        //将最后的，切掉
        if (sb.toString().endsWith(",")) {
            line=sb.substring(0,sb.length()-1);
        }

        outKey.set(key);
        outValue.set(line);
     context.write(outKey,outValue);
    }
}
