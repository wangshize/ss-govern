package org.ss.govern.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

/**
 * @author wangsz
 * @create 2020-07-19
 **/
public class FileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    /**
     * 持久化槽位分配数据到本地磁盘
     */
    public static Boolean persistSlotsAllocation(byte[] bytes, String fileDataDir, String filename) {
        try {
            File dataDir = new File(fileDataDir);
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

    public static void main(String[] args) throws Exception {
        String path = "/Users/wangsz/workspace/data/ss-01/slot_allocation";
        FileInputStream fis = new FileInputStream(new File(path));
        BufferedInputStream bis = new BufferedInputStream(fis);
        DataInputStream dis = new DataInputStream(bis);
        System.out.println("checksum:" + dis.readLong());
        int fileLength = dis.readInt();
        System.out.println("dataLength:" + fileLength);
        byte[] fileByte = new byte[fileLength];
        dis.readFully(fileByte);
        System.out.println(new String(fileByte));
    }

}
