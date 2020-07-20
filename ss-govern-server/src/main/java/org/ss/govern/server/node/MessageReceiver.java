package org.ss.govern.server.node;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.core.constants.NodeRequestType;
import org.ss.govern.server.node.master.Vote;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 通信数据接收组件
 * @author wangsz
 * @create 2020-07-15
 **/
public class MessageReceiver extends Thread {

    static final Logger LOGGER = LoggerFactory.getLogger(MessageReceiver.class);

    /**
     * master节点的网络通信组件
     */
    private NetworkManager networkManager;

    /**
     * 投票消息接收队列
     */
    private LinkedBlockingQueue<Vote> voteReceiveQueue =
            new LinkedBlockingQueue<>();
    /**
     * 槽位数据接收队列
     */
    private LinkedBlockingQueue<List<Integer>> slotsAllocationReceiveQueue =
            new LinkedBlockingQueue<>();

    public MessageReceiver(NetworkManager networkManager) {
        this.networkManager = networkManager;
    }

    @Override
    public void run() {
        while(NodeStatus.isRunning()) {
            try {
                ByteBuffer message = networkManager.takeMasterRecvMessage();
                int messageType = message.getInt();

                if (messageType == NodeRequestType.VOTE) {
                    Vote vote = new Vote(message);
                    voteReceiveQueue.put(vote);
                } else if (messageType == NodeRequestType.SLOTS_ALLOCATION) {
                    //获取剩余的数据，即除去总长度和类型之后
                    int remaining = message.remaining();
                    byte[] slotsAllocationByteArray = new byte[remaining];
                    message.get(slotsAllocationByteArray);
                    String slotsAllocationJSON = new String(slotsAllocationByteArray);
                    List<Integer> slotsAllocation = JSONObject.parseArray(slotsAllocationJSON, Integer.class);
                    slotsAllocationReceiveQueue.put(slotsAllocation);
                }
            } catch(Exception e) {
                LOGGER.error("receive message error......", e);
            }
        }
    }

    public Vote takeVote() {
        try {
            return voteReceiveQueue.take();
        } catch(Exception e) {
            LOGGER.error("take vote message error......", e);
            return null;
        }
    }

    public List<Integer> takeSlotsAllocation() {
        try {
            return slotsAllocationReceiveQueue.take();
        } catch(Exception e) {
            LOGGER.error("take slots allocation message error......", e);
            return null;
        }
    }
}
