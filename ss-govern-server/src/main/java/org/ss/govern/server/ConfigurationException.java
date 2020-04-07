package org.ss.govern.server;

/**
 * @author wangsz
 * @create 2020-04-05
 **/
public class ConfigurationException extends Exception {

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Exception e) {
        super(message, e);
    }
}
