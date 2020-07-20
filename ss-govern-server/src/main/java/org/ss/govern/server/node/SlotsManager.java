package org.ss.govern.server.node;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ss.govern.core.constants.Slot;
import org.ss.govern.server.config.GovernServerConfig;
import org.ss.govern.utils.FileUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 槽位数据管理
 * @author wangsz
 * @create 2020-07-19
 **/
public class SlotsManager {

    private static final Logger LOG = LoggerFactory.getLogger(SlotsManager.class);

    /**
     * 槽位分配存储文件的名字
     */
    private static final String SLOTS_ALLOCATION_FILENAME = "/slot_allocation";

    List<Slot> slots;

    private GovernServerConfig config = GovernServerConfig.getInstance();

    private MessageReceiver messageReceiver;

    public SlotsManager(MessageReceiver messageReceiver) {
        this.messageReceiver = messageReceiver;
    }

    public void waitForSlotsAllocation() {
        List<Integer> slotsAllocation = messageReceiver.takeSlotsAllocation();
        if(LOG.isDebugEnabled()) {
            LOG.debug("接收到槽位数据：" + slotsAllocation);
        }
        slots = slotsAllocation.stream()
                .map(slotIndex -> {
                    Slot slot = new Slot(slotIndex, config.getNodeId());
                    return slot;
                }).collect(Collectors.toList());
        //持久化槽位数据到磁盘
        String jsonSlots = JSONObject.toJSONString(slotsAllocation);
        byte[] slotsByte = jsonSlots.getBytes();
        FileUtils.persistSlotsAllocation(slotsByte, config.getDataDir(), SLOTS_ALLOCATION_FILENAME);
    }

    public Slot findSlot(Integer slotHash) {
        return slots.get(slotHash);
    }

}
