package org.ss.govern.core.constants;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author wangsz
 * @create 2020-07-19
 **/
@AllArgsConstructor
@Data
public class Slot {

    /**
     * 槽位编号
     */
    private Integer slotHash;

    /**
     * 节点地址
     */
    private Integer nodeId;

}
