package org.ss.govern.server.node.master;

import lombok.Getter;
import lombok.Setter;

/**
 * @author wangsz
 * @create 2020-07-04
 **/
public class MasterNodePeer {

    /**
     * 节点id
     */
    @Getter
    private Integer nodeId;
    /**
     * 是否为controller候选节点
     */
    @Setter
    @Getter
    private Boolean isControllerCandidate;

    public MasterNodePeer(Integer nodeId, Boolean isControllerCandidate) {
        this.nodeId = nodeId;
        this.isControllerCandidate = isControllerCandidate;
    }

}
