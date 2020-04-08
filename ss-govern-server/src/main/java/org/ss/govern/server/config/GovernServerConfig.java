package org.ss.govern.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.ConfigurationException;

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
     * master节点列表
     */
    private String masterNodeServers;

    /**
     * 解析配置文件
     *
     * @param configPath
     */
    public void parse(String configPath) throws ConfigurationException {
        try {
            LOG.info("going to parse configuration file: " + configPath);
            Properties configProperties;
            configProperties = loadConfigurationFIle(configPath);
            LOG.info("successfully loading configuration from file " + configPath);
            String nodeRole = configProperties.getProperty("node.role");
            if (ConfigValidates.checkNodeRole(nodeRole)) {
                this.nodeRole = nodeRole;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("debug parameter value : master.role=" + nodeRole);
                }
            }
            String masterNodeServers = configProperties.getProperty("master.node.servers");
            if(ConfigValidates.checkMasterNodeServers(masterNodeServers)) {
                this.masterNodeServers = masterNodeServers;
                if(LOG.isDebugEnabled()) {
                    LOG.debug("debug parameter value : master.node.servers=" + masterNodeServers);
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

    private Properties loadConfigurationFIle(String configPath) throws IOException {
        Properties configProperties;
        File configFile = new File(configPath);
        if (!configFile.exists()) {
            throw new IllegalArgumentException("config file " + configPath + " not found");
        }
        try (FileInputStream fis = new FileInputStream(configFile)) {
            configProperties = new Properties();
            configProperties.load(fis);

        }
        return configProperties;
    }

    public String getNodeRole() {
        return this.nodeRole;
    }
}
