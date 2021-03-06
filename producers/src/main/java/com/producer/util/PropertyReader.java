package com.producer.util;


import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyReader {
    private static final Logger logger = Logger.getLogger(PropertyReader.class);
    private static Properties prop = new Properties();
    public static Properties readPropertyFile() throws Exception {
        if (prop.isEmpty()) {
            InputStream input = PropertyReader.class.getClassLoader().getResourceAsStream("kafka-props.properties");
            try {
                prop.load(input);
            } catch (IOException ex) {
                logger.error(ex);
                throw ex;
            } finally {
                if (input != null) {
                    input.close();
                }
            }
        }
        return prop;
    }
}
