package com.roles.matrix.matrixMul;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class MatrixMulMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(MatrixMulMapper.class);

    private Text outKey = new Text();
    private Text outValue = new Text();

    private List<String> cacheList = new ArrayList<>();

    /**
     * 重写初始化方法
     *通过输入流将全局缓存中的右侧矩阵读入list中
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        //获取缓存
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        FileSystem  fileSystem = FileSystem.get(configuration);
        //根据cache获取一个输入流
        FSDataInputStream fs = fileSystem.open(path);

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs));
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = null;
        //每一行为 行 列_值T
        while ((line = bufferedReader.readLine()) != null) {
            cacheList.add(line);
        }
        reader.close();
        bufferedReader.close();

    }

    //每一行为 行 列_值
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] matrix1 = value.toString().split(" ");
        //行
        String row_matrix1 = matrix1[0];
        //列_值
        String[] cols_matrix1 = matrix1[1].split(",");

        for (String line : cacheList) {

            String[] matrix2 = line.toString().split("\t");
            //行
            String row_matrix2 = matrix2[0];
            //列_值
            String[] cols_matrix2 = matrix2[1].split(",");
            //两个矩阵行向量相乘
            int result = 0;
            //1_1 2_2 3_-2 4_0
            for (String column_value_matrix1 : cols_matrix1) {
                //遍历左侧矩阵的第一行第一列
                String column_matrix1 = column_value_matrix1.split("_")[0];
                String value_matrix1 = column_value_matrix1.split("_")[1];
                for (String column_value_matrix2 : cols_matrix2) {
                    //遍历右侧矩阵的一行
                    if (column_value_matrix2.startsWith(column_matrix1 + "_")) {
                        String value_matrix2 = column_value_matrix2.split("_")[1];
                        result += Integer.valueOf(value_matrix1) * Integer.valueOf(value_matrix2);
                    }
                }
            }
            //result是结果矩阵中的某一个元素，坐标为 行： row_matrix1,列 row_matrix2(因为右侧矩阵已经装置)
            outKey.set(row_matrix1);
            outValue.set(row_matrix2 + "_" + result);
            context.write(outKey, outValue);
        }
    }
}
