package com.roles.itemcf.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Reducer1 extends Reducer<Text,Text,Text,Text> {
    private Text outKey=new Text();
    private Text outValue=new Text();

    //去重，转换成关于item的矩阵
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String itemID=key.toString();

        Map<String,Integer> map=new HashMap<>();
        for (Text value:values) {
            String userID=value.toString().split("_")[0];
            String score=value.toString().split("_")[1];

            if (map.get(userID)==null){
                //第一次行为
                map.put(userID,Integer.valueOf(score));
            }else{
                Integer preScore=map.get(userID);
                //相当与点击两次
                map.put(userID,preScore+Integer.valueOf(score));
            }
        }

        StringBuilder sb=new StringBuilder();
        for (Map.Entry<String,Integer> entry:map.entrySet()) {
            String userID=entry.getKey();
            String score=String.valueOf(entry.getValue());
            sb.append(userID+"_"+score+",");
        }

        String line=null;
        if (sb.toString().endsWith(",")){
            line=sb.substring(0,sb.length()-1);
        }
        outKey.set(itemID);
        outValue.set(line);

        context.write(outKey,outValue);
    }
}
