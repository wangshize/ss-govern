package org.ss.govern.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.ConfigurationException;
import org.ss.govern.utils.ConfigValidates;
import org.ss.govern.utils.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * 配置管理
 *
 * @author wangsz
 * @create 2020-04-05
 **/
public class GovernServerConfig {

    private static final Logger LOG = LoggerFactory.getLogger(GovernServerConfig.class);

    private GovernServerConfig() {
    }

    private static class Singleton {
        static GovernServerConfig instance = new GovernServerConfig();
    }

    public static GovernServerConfig getInstance() {
        return Singleton.instance;
    }

    /**
     * 节点角色
     */
    private String nodeRole;

    /**
     * 解析配置文件
     *
     * @param configPath
     */
    public void parse(String configPath) throws ConfigurationException {
        try {
            File configFile = new File(configPath);
            if (!configFile.exists()) {
                throw new IllegalArgumentException("config file " + configPath + " not found");
            }
            try (FileInputStream fis = new FileInputStream(configFile)) {
                Properties configProperties = new Properties();
                configProperties.load(fis);
                LOG.info("successfully loading configuration from file " + configPath);
                String nodeRole = configProperties.getProperty("node.role");
                if (StringUtils.isNotEmpty(nodeRole)) {
                    if (!ConfigValidates.checkNodeRole(nodeRole)) {
                        throw new IllegalArgumentException("config node.type must be master or slave");
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("nodeRole in properties is " + nodeRole);
                    }
                    this.nodeRole = nodeRole;
                } else {
                    throw new IllegalArgumentException("config node.role can not be null");
                }
            }
            LOG.info("successfully validation all configuration entries");
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException("error processing " + configPath, e);
        } catch (FileNotFoundException e) {
            throw new ConfigurationException("file not found " + configPath, e);
        } catch (IOException e) {
            throw new ConfigurationException("error processing IO " + configPath, e);
        }
    }
}
