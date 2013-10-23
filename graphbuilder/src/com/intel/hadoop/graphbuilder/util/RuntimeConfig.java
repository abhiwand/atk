package com.intel.hadoop.graphbuilder.util;


import com.intel.hadoop.graphbuilder.job.AbstractCreateGraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RuntimeConfig {
    private static final Logger LOG = Logger.getLogger(RuntimeConfig.class);
    private static RuntimeConfig uniqueInstanceOfRuntimeConfig = null;
    private HashMap<String, String> config;
    private Configuration hadoopConf;
    private Properties properties;

    public static synchronized RuntimeConfig getInstance() {
        createInstance(null);
        return uniqueInstanceOfRuntimeConfig;
    }

    public static synchronized RuntimeConfig getInstance(Class klass) {
        createInstance(klass);
        return uniqueInstanceOfRuntimeConfig;
    }

    public static synchronized void createInstance(Class klass){
        if (uniqueInstanceOfRuntimeConfig == null) {
            if(klass != null){
                uniqueInstanceOfRuntimeConfig = new RuntimeConfig(klass);
            } else{
                uniqueInstanceOfRuntimeConfig = new RuntimeConfig();
            }
        } else {
            if(klass != null){
                uniqueInstanceOfRuntimeConfig.reflectSetConfigHash(klass);
            }
        }
    }

    private void initProperties() {
        if (properties == null) {
            properties = new Properties();
        }
    }

    private void initConfig() {
        if (config == null) {
            config = new HashMap<String, String>();
            hadoopConf = new Configuration();
        }
    }

    private RuntimeConfig() {
        initProperties();
        initConfig();
    }

    private RuntimeConfig(Class klass) {
        initProperties();
        initConfig();
        reflectSetConfigHash(klass);
    }

    private String splitKey(String key) {
        String[] nameSpacedKey = key.split("\\.");
        if (nameSpacedKey.length > 0) {
            String combinedKey = nameSpacedKey[1].toUpperCase();
            for (int i = 2; i < nameSpacedKey.length; i++) {
                combinedKey += "_" + nameSpacedKey[i].toUpperCase();
            }
            return combinedKey;
        } else {
            return null;
        }
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }

    public AbstractCreateGraphJob addConfig(AbstractCreateGraphJob conf){
        for(Map.Entry<String, String> option : hadoopConf.getValByRegex("^graphbuilder.").entrySet()){
            String key = option.getKey();
            String value = option.getValue();
            conf.addUserOpt(key, value);
        }
        return conf;
    }

    public void loadConfig(Reducer.Context context){
        loadConfig(context.getConfiguration());
    }
    public void loadConfig(Mapper.Context context) {
        loadConfig(context.getConfiguration());
    }

    public void loadConfig(Configuration conf) {
        if( conf != null){
            for (Map.Entry<String, String> option : conf.getValByRegex("^graphbuilder.").entrySet()) {
                String key = option.getKey();
                String value = option.getValue();
                hadoopConf.set(key, value);
                config.put(splitKey(key), value);
            }
        }
    }

    public Integer getPropertyInt(String property) {
        return Integer.parseInt(properties.getProperty(property, config.get(property)));
    }

    public String getPropertyString(String property) {
        return properties.getProperty(property, config.get(property));
    }

    public String getProperty(String property) {
        return getPropertyString(property);
    }

    private void reflectSetConfigHash(Class klass) {
        for (Field field : klass.getDeclaredFields()) {
            field.setAccessible(true);
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                Object value;
                try {
                    value = field.get(field.getType());
                } catch (IllegalAccessException e) {
                    value = null;
                }
                if (value != null && !config.containsKey(field.getName())) {
                    config.put(field.getName(), value.toString());
                }
            }
        }
    }
}
