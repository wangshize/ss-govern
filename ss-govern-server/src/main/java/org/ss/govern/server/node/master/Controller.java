package org.ss.govern.server.node.master;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.core.constants.NodeRequestType;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.server.node.NetworkManager;
import org.ss.govern.server.node.NodeManager;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

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
    private static final int SLOTS_COUNT = 16384;
    /**
     * 槽位分配存储文件的名字
     */
    private static final String SLOTS_ALLOCATION_FILENAME = "/slot_allocation";

    /**
     * 槽位分配数据  value:nodeId
     */
    private List<Integer> slotsAllocation = new ArrayList<>(SLOTS_COUNT);

    public Controller(NodeManager nodeManager, NetworkManager networkManager) {
        this.nodeManager = nodeManager;
        this.networkManager = networkManager;
    }

    /**
     * 分配槽位并持久化磁盘和发送其他节点
     */
    public void allocateSlots() {
        List<MasterNodePeer> masterNodePeers = nodeManager.getAllRemoteMasterNodes();
        int totalRemoteMasterNodeCount = masterNodePeers.size();
        int slotsPerNode = SLOTS_COUNT / totalRemoteMasterNodeCount;
        //分配槽位
        int slotIndex = 0;
        //为所有远程节点分配槽位
        for (MasterNodePeer masterNodePeer : masterNodePeers) {
            for (; slotIndex < slotsPerNode; slotIndex++) {
                slotsAllocation.add(slotIndex, masterNodePeer.getNodeId());
            }
        }
        //剩余槽位分配给Controller
        for(;slotIndex < SLOTS_COUNT; slotIndex++) {
            slotsAllocation.add(slotIndex, config.getNodeId());
        }
        //持久化分配数据到磁盘
        String jsonString = JSONObject.toJSONString(slotsAllocation);
        byte[] slotsByte = jsonString.getBytes();
        persistSlotsAllocation(slotsByte, SLOTS_ALLOCATION_FILENAME);
        //将分配好的槽位发送给其他master节点
        for (MasterNodePeer masterNodePeer : masterNodePeers) {
            int messageLength =  4 + slotsByte.length;
            ByteBuffer slotsAllocationByteBuffer =
                    ByteBuffer.allocate(messageLength);
            slotsAllocationByteBuffer.putInt(NodeRequestType.SLOTS_ALLOCATION);
            slotsAllocationByteBuffer.put(slotsByte);
            networkManager.sendMessage(masterNodePeer.getNodeId(), slotsAllocationByteBuffer);
        }
    }

    /**
     * 持久化槽位分配数据到本地磁盘
     */
    private Boolean persistSlotsAllocation(byte[] bytes, String filename) {
        try {
            File dataDir = new File(config.getDataDir());
            if(!dataDir.exists()) {
                dataDir.mkdirs();
            }
            File slotAllocationFile = new File(dataDir, filename);
            FileOutputStream fos = new FileOutputStream(slotAllocationFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            DataOutputStream dos = new DataOutputStream(bos);
            // 在磁盘文件里写入一份checksum校验和
            Checksum checksum = new Adler32();
            checksum.update(bytes, 0, bytes.length);
            long checksumValue = checksum.getValue();
            dos.writeLong(checksumValue);
            dos.writeInt(bytes.length);
            dos.write(bytes);
            // 对输出流进行一系列的flush，保证数据落地磁盘
            // 之前用DataOutputStream输出的数据都是进入了BufferedOutputStream的缓冲区
            // 所以在这里进行一次flush，数据就是进入底层的FileOutputStream
            bos.flush();
            //FileOutputStreamd flush 保证数据进入os cache
            fos.flush();
            //强制刷到磁盘
            fos.getChannel().force(false);
        } catch (Exception e) {
            LOG.error("persist slots allocation error......", e);
            return false;
        }
        return true;
    }

}
