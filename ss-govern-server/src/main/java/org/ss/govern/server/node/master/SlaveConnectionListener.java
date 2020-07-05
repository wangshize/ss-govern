package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.node.AbstractConnectionListener;
import org.ss.govern.server.node.NetworkManager;
import org.ss.govern.server.node.NodePeer;
import org.ss.govern.server.node.NodeStatus;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * slave节点网络连接监听器，监听slave的连接
 *
 * @author wangsz
 * @create 2020-04-10
 **/
public class SlaveConnectionListener extends AbstractConnectionListener {

    private final Logger LOG = LoggerFactory.getLogger(MasterConnectionListener.class);

    private NetworkManager networkManager;

    public SlaveConnectionListener(NetworkManager networkManager) {
        super(networkManager);
        this.networkManager = networkManager;
        init();
    }

    private void init() {
        NodePeer self = networkManager.getSelf();
        if (self != null) {
            bindPort = self.getSlaveConnectPort();
        } else {
            NodeStatus nodeStatus = NodeStatus.getInstance();
            nodeStatus.setStatus(NodeStatus.FATAL);
        }
    }

    @Override
    protected void doAccept(Socket client) {
        //初次建立连接，启动针对slave的读写线程
        Integer remoteNodeId = networkManager.receiveSlaveConnection(client);
        LOG.info("accept slave node id : " + remoteNodeId);
        if (remoteNodeId != null) {
            networkManager.startSlvaeSocketIOThreads(remoteNodeId, client);
        }
    }

}