package org.ss.govern.server.config;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.ConfigurationException;
import org.ss.govern.server.node.NodeInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
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
     * 节点编号
     */
    private Integer nodeId;

    /**
     * 是否为controller候选节点
     * */
    private Boolean isControllerCandidate;

    /**
     * 解析配置文件
     *
     * @param configPath
     */
    public void parse(String configPath) throws ConfigurationException {
        try {
            LOG.info("going to parse configuration file: " + configPath);
            Properties configProperties;
            configProperties = loadConfigurationFile(configPath);
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
            String nodeId = configProperties.getProperty("node.id");
            if(ConfigValidates.checkNodeId(nodeId)) {
                this.nodeId = Integer.valueOf(nodeId);
                if(LOG.isDebugEnabled()) {
                    LOG.debug("debug parameter value : node.id=" + nodeId);
                }
            }
            String isControllerCandidate = configProperties.getProperty("is.controller.candidate");
            if(ConfigValidates.checkIsControllerCandidate(isControllerCandidate)) {
                if(StringUtils.isEmpty(isControllerCandidate)) {
                    this.isControllerCandidate = Boolean.TRUE;
                } else {
                    this.isControllerCandidate = Boolean.valueOf(isControllerCandidate);
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("debug parameter value : is.controller.candidate=" + isControllerCandidate);
                    }
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

    private Properties loadConfigurationFile(String configPath) throws IOException {
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

    public Integer getNodeId() {
        return this.nodeId;
    }

    public String getMasterNodeServers() {
        return this.masterNodeServers;
    }

    public Boolean getIsControllerCandidate() {
        return isControllerCandidate;
    }
}
