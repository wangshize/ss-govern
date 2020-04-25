package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.node.NodeStatus;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * master节点间网络连接读线程
 * 每两个节点之间启动一个读线程
 * @author wangsz
 * @create 2020-04-11
 **/
public class MasterNetworkReadThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(MasterNetworkReadThread.class);

    private Socket socket;

    private Integer remoteNodeId;

    private MasterNetworkManager manager;

    private DataInputStream inputStream;

    public MasterNetworkReadThread(Integer remoteNodeId, Socket socket,
                                   MasterNetworkManager masterNetworkManager) {
        this.manager = masterNetworkManager;
        this.remoteNodeId = remoteNodeId;
        this.socket = socket;
        try {
            this.inputStream = new DataInputStream(socket.getInputStream());
            socket.setSoTimeout(0);
        } catch (IOException e) {
            LOG.error("Error while accessing socket for " + remoteNodeId, e);
            manager.closeSocket(socket);
            NodeStatus.fatal();
        }
    }

    synchronized void finish() {
        if(!NodeStatus.isRunning()){
            return;
        }
        NodeStatus.fatal();
        this.interrupt();
    }

    @Override
    public void run() {
        LOG.info("start a read IO thread for remote node:" + socket.getRemoteSocketAddress());
        while (NodeStatus.isRunning()) {
            try {
                int messageLength = inputStream.readInt();
                byte[] messageByte = new byte[messageLength];
                inputStream.readFully(messageByte, 0, messageLength);
                ByteBuffer messageBuffer = ByteBuffer.wrap(messageByte);
                manager.addToRecvQueue(messageBuffer);
                if(LOG.isDebugEnabled()) {
                    LOG.debug("receive message from node : " + socket.getRemoteSocketAddress()
                            + ", message size is " + messageBuffer.capacity());
                }
            } catch (IOException e) {
                LOG.error("read data from remote node error", e);
                finish();
            } catch (InterruptedException e) {
                LOG.error("put message into recvQueue error", e);
            }
        }
    }
}
