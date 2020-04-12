package org.ss.govern.server.node.master;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.config.ConfigurationParser;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.NodeInfo;
import org.ss.govern.server.node.NodeStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 集群节点间的通信管理组件
 * master和master  master和slave之间通信
 * 1、和其他master节点建立网络连接，避免出现重复的链接
 * 2、底层基于队列和线程，发送请求给其他节点，接收其他节点
 * 发送过来的请求放入接收队列
 * @author wangsz
 * @create 2020-04-09
 **/
public class MasterNetworkManager {

    private static final Logger LOG = LoggerFactory.getLogger(MasterNetworkManager.class);

    private final int DEFAULT_RETRIES = 3;
    private final int CONNECT_TIMEOUT = 5000;

    /**
     * 等待重试发起连接的master列表
     */
    private List<NodeInfo> retryConnectOtherMasterNodes = new CopyOnWriteArrayList<>();

    public MasterNetworkManager() {
        new  RetryConnectMasterNodeThread().start();
    }

    public void waitOtherMasterNodesConnect() {
        new MasterConnectionListener().start();
    }

    public Boolean connectOtherMasterNodes() {
        List<NodeInfo> beforeMasterNodes = getBeforeMasterNodes();
        if(CollectionUtils.isEmpty(beforeMasterNodes)) {
            return true;
        }
        for (NodeInfo beforeMasterNode : beforeMasterNodes) {
            connectBeforeMasterNode(beforeMasterNode);
        }
        return false;
    }

    private boolean connectBeforeMasterNode(NodeInfo nodeInfo) {
        int retries = 0;
        String ip = nodeInfo.getIp();
        int port = nodeInfo.getMasterConnectPort();
        LOG.info("try to connect master node :" + ip + ":" + port);
        while (NodeStatus.isRunning() && retries <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(ip, port);
                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(0);
                socket.connect(endpoint, CONNECT_TIMEOUT);
                LOG.info("connect with");
                //连接建立成功，启动读写io线程
                new MasterNetworkReadThread(socket).start();
                new MasterNetworkWriteThread(socket).start();
                LOG.info("successfully connected master node :" + ip + ":" + port);
                return true;
            } catch (IOException e) {
                LOG.error("connect with " + nodeInfo.getIp() + " fail");
                retries++;
                if(retries <= DEFAULT_RETRIES) {
                    LOG.info(String.format("connect with %s fail, retry to connect %s times",
                            nodeInfo.getIp(), retries));
                }
            }
        }
        if(!retryConnectOtherMasterNodes.contains(nodeInfo)) {
            retryConnectOtherMasterNodes.add(nodeInfo);
            LOG.error("retried connect master node :" + ip + ":" + port + "failed, " +
                    "and it into retry connect list");
        }
        return false;
    }

    /**
     * 获取id比自己小的节点信息列表
     * @return
     */
    private List<NodeInfo> getBeforeMasterNodes() {
        ConfigurationParser parser = ConfigurationParser.getInstance();
        List<NodeInfo> nodeInfoList = parser.parseMasterNodeServers();
        Integer nodeId = GovernServerConfig.getInstance().getNodeId();
        return nodeInfoList.subList(0, nodeId);
    }

    class RetryConnectMasterNodeThread extends Thread {
        private final Logger LOG = LoggerFactory.getLogger(RetryConnectMasterNodeThread.class);
        private final int RETRY_CONNECT_MASTER_NODE_INTERVAL = 5 * 60 * 1000;

        @Override
        public void run() {
            while (NodeStatus.isRunning()) {
                List<NodeInfo> retryConnectSuccessNodes = new ArrayList<>();
                for (NodeInfo nodeInfo : retryConnectOtherMasterNodes) {
                    if(connectBeforeMasterNode(nodeInfo)) {
                        retryConnectSuccessNodes.add(nodeInfo);
                    }
                }
                for (NodeInfo successNode : retryConnectSuccessNodes) {
                    retryConnectOtherMasterNodes.remove(successNode);
                }
                try {
                    Thread.sleep(RETRY_CONNECT_MASTER_NODE_INTERVAL);
                } catch (InterruptedException e) {
                    LOG.error("retryConnectMasterNodeThread Interrupted while sleeping. " +
                            "Ignoring exception", e);
                }
            }
        }
    }

}
