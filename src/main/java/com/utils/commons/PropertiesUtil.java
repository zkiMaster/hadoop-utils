package com.utils.commons;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {

    private static Properties properties=new Properties();

    public PropertiesUtil(String fileName){
        open(fileName);
    }
    public static Object getValueByBundle(String location,String property){
        open(location);
        return properties.get(property);
    }

    private static void open(String fileName){
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public String readPropertyByKey(String key){
        return properties.getProperty(key);
    }
}
