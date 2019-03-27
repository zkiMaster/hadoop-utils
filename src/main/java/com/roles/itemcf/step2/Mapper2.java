package com.roles.itemcf.step2;

import com.utils.jobutils.VectorUtil;
import com.roles.matrix.matrixMul.MatrixMulMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger log = LoggerFactory.getLogger(MatrixMulMapper.class);
    private VectorUtil vectorUtil = new VectorUtil();

    private Text outKey = new Text();
    private Text outValue = new Text();
    private List<String> cacheList = new ArrayList<>();

    private DecimalFormat df = new DecimalFormat("0.00");


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        FileSystem fs = FileSystem.get(configuration);
        URI[] cacheFiles = context.getCacheFiles();
        InputStream inputStream = fs.open(new Path(cacheFiles[0]));
        //通过输入流将全局缓存中的右侧矩阵读入list中
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        //每一行为 行 列_值T
        while ((line = bufferedReader.readLine()) != null) {
            cacheList.add(line);
        }
        inputStream.close();
        bufferedReader.close();

    }

    //每一行为 行 列_值
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] matrix1 = value.toString().split("\t");
        String row_matrix1 = matrix1[0];
        String[] cols_matrix1 = matrix1[1].split(",");
        double denominator1 = vectorUtil.getVectorLen(cols_matrix1, 0);

        for (String line : cacheList) {
            String[] matrix2 = line.toString().split("\t");
            String row_matrix2 = matrix2[0];
            String[] cols_matrix2 = matrix2[1].split(",");
            double denominator2 = vectorUtil.getVectorLen(cols_matrix2, 0);
            //两个矩阵行向量相乘
            int numertor = 0;
            //1_1 2_2 3_-2 4_0
            for (String column_value_matrix1 : cols_matrix1) {
                //遍历左侧矩阵的第一行第一列
                String column_matrix1 = column_value_matrix1.split("_")[0];
                String value_matrix1 = column_value_matrix1.split("_")[1];
                for (String column_value_matrix2 : cols_matrix2) {
                    //遍历右侧矩阵的一行
                    if (column_value_matrix2.startsWith(column_matrix1 + "_")) {
                        String value_matrix2 = column_value_matrix2.split("\t")[1];
                        numertor += Integer.valueOf(value_matrix1) * Integer.valueOf(value_matrix2);
                    }
                }
            }
            double cos = numertor / (denominator1 * denominator2);
            if (cos == 0) {
                continue;
            }
            //result是结果矩阵中的某一个元素，坐标为 行： row_matrix1,列 row_matrix2(因为右侧矩阵已经装置)
            outKey.set(row_matrix1);
            outValue.set(row_matrix2 + "_" + df.format(cos));
            context.write(outKey, outValue);
        }
    }


}
