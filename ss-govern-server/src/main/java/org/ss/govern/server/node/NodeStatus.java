package org.ss.govern.server.node;

/**
 * @author wangsz
 * @create 2020-04-08
 **/
public class NodeStatus {

    public static final int INITIALIZING = 0;
    public static final int RUNNING = 1;
    public static final int SHUTDOWN = 2;
    public static final int FATAL = 3;

    public static final int SHUTDOWN_CHECK_INTERVAL = 300;

    private NodeStatus() {
    }

    public static NodeStatus getInstance() {
        return Singleton.instance;
    }

    public static boolean isRunning() {
        return NodeStatus.RUNNING == NodeStatus.get();
    }

    /**
     * 节点状态
     */
    private volatile int status;

    public void setStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return this.status;
    }

    public static int get() {
        return getInstance().getStatus();
    }

    private static class Singleton {
        static NodeStatus instance = new NodeStatus();
    }
}
