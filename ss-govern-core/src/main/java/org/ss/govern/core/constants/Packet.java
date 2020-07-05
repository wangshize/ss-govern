package org.ss.govern.core.constants;

/**
 * 通信数据包
 * @author wangsz
 * @create 2020-05-27
 **/
public class Packet {

    private final Integer magic = 0Xcf;

    private Long length;

    private String version;

    private String type;

    private byte[] message;
}
