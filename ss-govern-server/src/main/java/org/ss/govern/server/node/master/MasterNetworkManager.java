package org.ss.govern.server.node.master;

/**
 * 集群节点间的通信管理组件
 * master和master  master和slave之间通信
 * 1、和其他master节点建立网络连接，避免出现重复的链接
 * 2、底层基于队列和线程，发送请求给其他节点，接收其他节点
 * 发送过来的请求放入接收队列
 * @author wangsz
 * @create 2020-04-09
 **/
public class MasterNetworkManager {

    public void waitOtherMasterNodesConnect() {

    }

    public Boolean connectOtherMasterNodes() {

        return false;
    }

}
