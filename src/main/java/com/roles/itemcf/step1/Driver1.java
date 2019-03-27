package com.roles.itemcf.step1;

import com.utils.jobutils.JobUtil;
import org.apache.hadoop.io.Text;


public class Driver1 {
    public static void main(String[] args) {
        JobUtil jobUtil = new JobUtil();
        jobUtil.setConf("fs.defaultFS", "hdfs://192.168.10.121:9000")
                .setJarByClass(Driver1.class)
                .setMapper(Mapper1.class, Text.class, Text.class)
                .setReduce(Reducer1.class, Text.class, Text.class)
                .setInput("matrix/input/itemCF/step1_input/ActionList.txt")
                .setOutPut("matrix/output/itemCF/step1_output")
                .commit(true);
    }
}
