package org.ss.govern.server.node.master;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.core.constants.NodeRequestType;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.NetworkManager;
import org.ss.govern.server.node.NodeManager;
import org.ss.govern.utils.FileUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangsz
 * @create 2020-07-09
 **/
public class Controller {
    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);

    private NodeManager nodeManager;

    private NetworkManager networkManager;

    private GovernServerConfig config = GovernServerConfig.getInstance();

    /**
     * slot槽位的总数量
     */
    private static final int SLOTS_COUNT = 20;
    /**
     * 槽位分配存储文件的名字
     */
    private static final String SLOTS_ALLOCATION_FILENAME = "/slot_allocation";

    public Controller(NodeManager nodeManager, NetworkManager networkManager) {
        this.nodeManager = nodeManager;
        this.networkManager = networkManager;
    }

    /**
     * 分配槽位并持久化磁盘和发送其他节点
     */
    public void allocateSlots() {
        List<MasterNodePeer> masterNodePeers = nodeManager.getAllRemoteMasterNodes();
        int totalMasterNodeCount = masterNodePeers.size() + 1;
        int slotsPerNode = SLOTS_COUNT / totalMasterNodeCount;
        //element：nodeId
        List<Integer> slotsAllocation = allocationSlots(masterNodePeers, slotsPerNode);
        String jsonString = JSONObject.toJSONString(slotsAllocation);
        byte[] slotsByte = jsonString.getBytes();
        //持久化分配数据到磁盘
        FileUtils.persistSlotsAllocation(slotsByte, config.getDataDir(), SLOTS_ALLOCATION_FILENAME);
        //将分配好的槽位发送给其他master节点
        syncSlotsAllocation(masterNodePeers, slotsByte);
    }

    protected List<Integer> allocationSlots(List<MasterNodePeer> masterNodePeers, int slotsPerNode) {
        //element：nodeId
        List<Integer> slotsAllocation = new ArrayList<>(SLOTS_COUNT);
        //分配槽位
        int slotIndex = 0;
        //为所有远程节点分配槽位
        for (MasterNodePeer masterNodePeer : masterNodePeers) {
            for (int allocatedCount = 1; allocatedCount <= slotsPerNode; slotIndex++,allocatedCount++) {
                slotsAllocation.add(slotIndex, masterNodePeer.getNodeId());
            }
        }
        //剩余槽位分配给Controller
        for(;slotIndex < SLOTS_COUNT; slotIndex++) {
            slotsAllocation.add(slotIndex, config.getNodeId());
        }
        return slotsAllocation;
    }

    protected void syncSlotsAllocation(List<MasterNodePeer> masterNodePeers, byte[] slotsByte) {
        for (MasterNodePeer masterNodePeer : masterNodePeers) {
            int messageLength =  4 + slotsByte.length;
            ByteBuffer slotsAllocationByteBuffer =
                    ByteBuffer.allocate(messageLength);
            slotsAllocationByteBuffer.putInt(NodeRequestType.SLOTS_ALLOCATION);
            slotsAllocationByteBuffer.put(slotsByte);
            networkManager.sendMessage(masterNodePeer.getNodeId(), slotsAllocationByteBuffer);
        }
    }

}
