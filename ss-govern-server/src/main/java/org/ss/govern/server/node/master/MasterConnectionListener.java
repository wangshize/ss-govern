package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.node.AbstractConnectionListener;
import org.ss.govern.server.node.NetworkManager;
import org.ss.govern.server.node.NodeAddress;
import org.ss.govern.server.node.NodeStatus;

import java.net.Socket;

/**
 * 网络连接监听器
 *
 * @author wangsz
 * @create 2020-04-10
 **/
public class MasterConnectionListener extends AbstractConnectionListener {

    private final Logger LOG = LoggerFactory.getLogger(MasterConnectionListener.class);

    private NetworkManager networkManager;

    public MasterConnectionListener(NetworkManager networkManager) {
        super(networkManager);
        this.networkManager = networkManager;
        init();
    }

    private void init() {
        NodeAddress self = networkManager.getSelf();
        if (self != null) {
            bindPort = self.getMasterConnectPort();
        } else {
            NodeStatus nodeStatus = NodeStatus.getInstance();
            nodeStatus.setStatus(NodeStatus.FATAL);
        }
    }

    @Override
    protected void doAccept(Socket client) {
        //初次建立连接，启动针对其他master的读写线程
        Integer remoteNodeId = networkManager.receiveMasterConnection(client);
        LOG.info("accept master node id : " + remoteNodeId);
        if (remoteNodeId != null) {
            networkManager.startMasterSocketIOThreads(remoteNodeId, client);
        }
    }


}