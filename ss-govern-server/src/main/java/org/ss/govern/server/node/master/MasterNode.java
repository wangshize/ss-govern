package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.core.constants.MasterNodeRole;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.NetworkManager;
import org.ss.govern.server.node.NodeManager;

/**
 * @author wangsz
 * @create 2020-04-08
 **/
public class MasterNode {

    private static final Logger LOG = LoggerFactory.getLogger(MasterNode.class);


    private ControllerCandidate controllerCandidate;

    private NetworkManager networkManager;

    private NodeManager nodeManager;

    private GovernServerConfig serverConfig;

    public MasterNode() {
        this.nodeManager = new NodeManager();
        this.networkManager = new NetworkManager(nodeManager);
        this.controllerCandidate = new ControllerCandidate(networkManager, nodeManager);
        this.serverConfig = GovernServerConfig.getInstance();
    }

    public void start() throws InterruptedException {
        //等待id大于自己的节点来连接
        networkManager.waitOtherMasterNodesConnect();
        //连接id小于自己的master节点
        networkManager.connectOtherMasterNodes();
        //等待大多数节点启动
        networkManager.waitMostNodesConnected();
        //选举controller
        Boolean isControllerCandidate = serverConfig.getIsControllerCandidate();
        if (isControllerCandidate) {
            MasterNodeRole role = controllerCandidate.voteForControllerElection();
            LOG.info("vote finish, Current NodeRole is " + role);
        }
        //启动线程监听slave节点发起的连接请求
        networkManager.waitSlaveNodeConnect();
    }

}
