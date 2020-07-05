package org.ss.govern.server.node.slave;

import lombok.Getter;

/**
 * @author wangsz
 * @create 2020-07-04
 **/
public class SlaveNodePeer {

    /**
     * 远程节点id
     */
    @Getter
    private Integer nodeId;

    public SlaveNodePeer(Integer nodeId) {
        this.nodeId = nodeId;
    }

}
