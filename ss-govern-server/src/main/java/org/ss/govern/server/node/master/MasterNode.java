package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.node.NodeStatus;

/**
 * @author wangsz
 * @create 2020-04-08
 **/
public class MasterNode {

    private static final Logger LOG = LoggerFactory.getLogger(MasterNode.class);


    private ControllerCandidate controllerCandidate;

    private MasterNetworkManager masterNetworkManager;

    public MasterNode() {
        this.masterNetworkManager = new MasterNetworkManager();
        this.controllerCandidate = new ControllerCandidate(masterNetworkManager);
    }

    public void start() {
        //等待id大于自己的节点来连接
        masterNetworkManager.waitOtherMasterNodesConnect();
        //等待slave节点连接

        //连接id小于自己的master节点
        masterNetworkManager.connectOtherMasterNodes();

        masterNetworkManager.waitAllNodesConnected();
    }
}
