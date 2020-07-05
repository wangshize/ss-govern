package org.ss.govern.server.node.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.server.node.NetworkManager;
import org.ss.govern.server.node.NodeStatus;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * master节点间网络连接读线程
 * 每两个节点之间启动一个读线程
 * @author wangsz
 * @create 2020-04-11
 **/
public class NetworkReadThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkReadThread.class);

    private Socket socket;

    private Integer remoteNodeId;

    private LinkedBlockingQueue<ByteBuffer> queueRecv;

    private NetworkManager manager;

    private DataInputStream inputStream;

    public NetworkReadThread(Integer remoteNodeId, Socket socket,
                             LinkedBlockingQueue<ByteBuffer> queueRecv,
                             NetworkManager masterNetworkManager) {
        this.manager = masterNetworkManager;
        this.remoteNodeId = remoteNodeId;
        this.queueRecv = queueRecv;
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
                //todo  处理拆包问题
                byte[] messageByte = new byte[messageLength];
                inputStream.readFully(messageByte, 0, messageLength);
                ByteBuffer messageBuffer = ByteBuffer.wrap(messageByte);
                messageBuffer.rewind();
                queueRecv.put(messageBuffer);
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
