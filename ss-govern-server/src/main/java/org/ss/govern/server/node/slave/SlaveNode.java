package org.ss.govern.server.node.slave;

import org.ss.govern.server.node.NetworkManager;
import org.ss.govern.server.node.NodeManager;

/**
 * @author wangsz
 * @create 2020-04-08
 **/
public class SlaveNode {

    private SlaveNetworkManager networkManager;

    public SlaveNode() {
        this.networkManager = new SlaveNetworkManager();
    }

    public void start() {
        //连接master节点
        networkManager.connectMasterNode();
    }
}
