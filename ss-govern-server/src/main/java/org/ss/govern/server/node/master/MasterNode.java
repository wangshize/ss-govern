package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.RemoteNodeManager;

/**
 * @author wangsz
 * @create 2020-04-08
 **/
public class MasterNode {

    private static final Logger LOG = LoggerFactory.getLogger(MasterNode.class);


    private ControllerCandidate controllerCandidate;

    private NetworkManager masterNetworkManager;

    private RemoteNodeManager remoteNodeManager;

    private GovernServerConfig serverConfig;

    public MasterNode() {
        this.remoteNodeManager = new RemoteNodeManager();
        this.masterNetworkManager = new NetworkManager(remoteNodeManager);
        this.controllerCandidate = new ControllerCandidate(masterNetworkManager, remoteNodeManager);
        this.serverConfig = GovernServerConfig.getInstance();
    }

    public void start() throws InterruptedException {
        //等待id大于自己的节点来连接
        masterNetworkManager.waitOtherMasterNodesConnect();
        //连接id小于自己的master节点
        masterNetworkManager.connectOtherMasterNodes();
        //等待大多数节点启动
        masterNetworkManager.waitMostNodesConnected();
        //选举controller
        Boolean isControllerCandidate = serverConfig.getIsControllerCandidate();
        if(isControllerCandidate) {
            Integer role = controllerCandidate.voteForControllerElection();
            LOG.info("vote finish,masterNodeRole is " + role);
        }
        //启动线程监听slave节点发起的连接请求
    }
}
