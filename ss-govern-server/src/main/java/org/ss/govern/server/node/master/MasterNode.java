package org.ss.govern.server.node.master;

/**
 * @author wangsz
 * @create 2020-04-08
 **/
public class MasterNode {

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
    }
}
