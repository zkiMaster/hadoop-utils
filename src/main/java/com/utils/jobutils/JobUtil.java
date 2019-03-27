package com.utils.jobutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JobUtil {

    private static final Logger log = LoggerFactory.getLogger(JobUtil.class);
    private Configuration conf;
    private Job job = null;

    public JobUtil() {
        this.conf = new Configuration();
        getJob(conf);
    }

    public JobUtil(Configuration conf) {
        getJob(conf);
    }

    public JobUtil setConf(Configuration conf) {
        this.job = null;
        getJob(conf);
        return this;
    }

    public JobUtil setConf(String key, Object value) {
        this.job = null;
        if (value instanceof String) {
            conf.set(key, (String) value);
        } else if (value instanceof Integer) {
            conf.setInt(key, (Integer) value);
        } else if (value instanceof Long) {
            conf.setLong(key, (Long) value);
        } else if (value instanceof Double) {
            conf.setDouble(key, (Double) value);
        } else if (value instanceof Float) {
            conf.setDouble(key, (Float) value);
        }
        getJob(conf);
        return this;
    }

    public Job getJob(){
        return job;
    }

    public Job getJob(Configuration conf) {
        try {
            if (job == null) {
                synchronized (JobUtil.class) {
                    if (job == null) {
                        job = Job.getInstance(conf);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    public JobUtil setJarByClass(Class jarClass) {
        job.setJarByClass(jarClass);
        return this;
    }

    public JobUtil setMapper(Class mapperClass, Class keyClass, Class valueClass) {
        log.info("MapperClass: " + mapperClass.getSimpleName() + " ,MapOutputKeyClass<>: "
                + keyClass.getSimpleName() + " ,MapOutputValueClass<>: " + valueClass.getSimpleName());
        job.setMapperClass(mapperClass);
        job.setMapOutputKeyClass(keyClass);
        job.setMapOutputValueClass(valueClass);
        return this;
    }

    public JobUtil setReduce(Class reduceClass, Class keyClass, Class valueClass) {
        log.info("ReducerClass: " + reduceClass.getSimpleName() + " ,OutputKeyClass<最终输出结果的key类型>: "
                + keyClass.getSimpleName() + " ,OutputValueClass<最终输出结果的value类型>: " + valueClass.getSimpleName());
        job.setReducerClass(reduceClass);
        job.setOutputKeyClass(keyClass);
        job.setOutputValueClass(valueClass);
        return this;
    }

    public JobUtil setInput(String path) {
        log.info("输入路径：" + path);
        try {
            FileInputFormat.setInputPaths(job, path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    public JobUtil setOutPut(String path) {
        log.info("输出路径：" + path);
        Configuration configuration = job.getConfiguration();
        try {
            FileSystem fs = FileSystem.get(configuration);
            boolean exists = fs.exists(new Path(path));
            if (exists)
                fs.delete(new Path(path),true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        FileOutputFormat.setOutputPath(job, new Path(path));
        return this;
    }

    public boolean commit(boolean verbose) {
        try {
            return job.waitForCompletion(verbose);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

}
