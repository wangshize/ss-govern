package org.ss.govern.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.utils.StringUtils;

/**
 * 服务治理平台server
 *
 * @author wangsz
 * @create 2020-04-05
 **/
public class GovernServer {

    private static final Logger LOG = LoggerFactory.getLogger(GovernServer.class);

    /**
     * 服务治理平台server启动类
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            String configPath = args[0];
            if (StringUtils.isNotEmpty(configPath)) {
                GovernServerConfig config = GovernServerConfig.getInstance();
                LOG.info("going to parse configuration file: " + configPath);
                config.parse(configPath);
            } else {
                throw new ConfigurationException("configuration file cannot be empty......");
            }
            LOG.info("finish parsing configuration file: " + configPath);
        } catch (ConfigurationException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.exit(2);
        }
    }

}
