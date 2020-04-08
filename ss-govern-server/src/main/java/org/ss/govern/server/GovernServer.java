package org.ss.govern.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.core.constants.NodeRole;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.NodeStatus;
import org.ss.govern.server.node.master.MasterNode;
import org.ss.govern.server.node.slave.SlaveNode;
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
            LOG.info("starting govern server......");
            String configPath = args[0];
            GovernServerConfig config = GovernServerConfig.getInstance();
            if (StringUtils.isEmpty(configPath)) {
                throw new ConfigurationException("configuration file cannot be empty");
            }
            NodeStatus nodeStatus = NodeStatus.getInstance();
            nodeStatus.setStatus(NodeStatus.INITIALIZING);
            config.parse(configPath);
            String nodeRole = config.getNodeRole();
            startNode(nodeRole);
            nodeStatus.setStatus(NodeStatus.RUNNING);
            waitForShutdown();
        } catch (ConfigurationException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.exit(2);
        } catch (InterruptedException e) {
            LOG.error("encounter thread interrupt error", e);
            System.exit(1);
        }
    }

    private static void startNode(String nodeRole) {
        if(NodeRole.MASTER.equals(nodeRole)) {
            MasterNode master = new MasterNode();
            master.start();
        } else if(NodeRole.SLAVE.equals(nodeRole)) {
            SlaveNode slave = new SlaveNode();
            slave.start();
        }
    }

    private static void waitForShutdown() throws InterruptedException {
        while (NodeStatus.RUNNING == NodeStatus.get()) {
            Thread.sleep(NodeStatus.SHUTDOWN_CHECK_INTERVAL);
        }
    }

}
