package com.guhanjie;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 分布式全局唯一ID生成器
 * 
 * <p>
 * ID由8字节（64bit）组成：32位时间戳+16位顺序号+8位地域号+8位节点号
 * <ul>
 * <li>a 32 bit time stamp in seconds from epoch(2020-01-01),</li>
 * <li>a 16 bit sequence counter(auto increment by cyclic),</li>
 * <li>a  8 bit region identifier(region or IDC)</li>
 * <li>a  8 bit node identifier(machine or process)</li>
 * </ul>
 * 
 * <pre>
 *  0xFFFFFFFF00000000 time stamp(seconds from epoch 2020, can till 2156)
 *  0x00000000FFFF0000 sequence(limit 64K TPS in the same second per node, can have a burst promotion)
 *  0x000000000000FF00 region(support max 256 global regions)
 *  0x00000000000000FF node(support max 256 nodes per region, including machine or process)
 * </pre>
 * 
 * @author ghj
 * @since 2020-03-10 17:06:04
 */
public class DistributedIdGenerator {

    private static final long SEQUENCE_MASK = 0xFFFFL;
    private static final long REGION_MASK = 0xFFL;

    private static final int TIMESTAMP_BIT_OFFSET = 32;
    private static final int SEQUENCE_BIT_OFFSET = 16;
    private static final int REGION_BIT_OFFSET = 8;
    private static final int NODE_ID_BIT_OFFSET = 0;

    private static final long EPOCH = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Z")).toEpochSecond();
    private static final long REGION_ID;
    private static final long NODE_ID;
    private static final Map<String, Long> NODE_MAP = new HashMap<String, Long>();

    private static long lastTimestamp = 0; // last time stamp in second
    private static long lastTimestampUpdated = 0; // last updated time stamp in second
    private static int sequence = 0; //sequence number, keep strictly increasing in same second
    private static final int TIME_UPDATE_INTERVAL_IN_SECOND = 10; // persist rate for time stamp update

    static {
        NODE_ID = initNodeId();     // initialize for NODE_ID
        REGION_ID = initRegionId(); // initialize for REGION_ID
        initLastTimestamp();        // initialize for lastTimestamp
    }

    /**
     * 生成全局唯一ID（long）
     * @return
     * @author    ghj
     * @since     2020-03-12 23:53:38
     */
    public static long nextId() {
        return nextId(System.currentTimeMillis() / 1000);
    }

    /**
     * 生成全局唯一ID（String）
     * @return
     * @author    ghj
     * @since     2020-03-12 23:54:08
     */
    public static String nextString() {
        return Long.toHexString(nextId());
    }

    private static synchronized long nextId(long timeSecond) {
        if (timeSecond < lastTimestamp) { // 处理时钟回拨，保证时钟始终严格单调递增
            // warning: clock is turn back!
            System.err.println("clock is back, current:"+timeSecond+", last:"+lastTimestamp);
            timeSecond = lastTimestamp;
        }
        if (timeSecond != lastTimestamp) {
            // 以一定频率持久化更新lastTimestamp
            // 保证新时间在超过上次更新时间+更新间隔时必须在持久化后才开始发放Seq号
            if (timeSecond-lastTimestampUpdated >= TIME_UPDATE_INTERVAL_IN_SECOND) {
                lastTimestampUpdated = timeSecond;
                // updateToDB(lastTimestampUpdated);
            }
            // ==============================================
            lastTimestamp = timeSecond;
            sequence = 0; // 递增序列号清零
        }
        sequence++;
        long next = sequence & SEQUENCE_MASK;
        if (next == 0) { // 同一秒内sequence超过64K，向下一秒借sequence
            System.err.println("sequence reached maximum in 1 second:" + timeSecond);
            return nextId(timeSecond + 1);
        }
        return generateId(timeSecond, next, REGION_ID, NODE_ID);
    }

    private static long generateId(long epochSecond, long next, long regionId, long nodeId) {
        return ((epochSecond - EPOCH) << TIMESTAMP_BIT_OFFSET) | (next << SEQUENCE_BIT_OFFSET)
                | (regionId << REGION_BIT_OFFSET) | (nodeId << NODE_ID_BIT_OFFSET);
    }

    /**
     * 启动时手动注册地域编号，为确保唯一标识，可做静态配置，也可从配置中心或DB中加载
     * @return
     * @author    ghj
     * @since     2020-03-13 00:02:57
     */
    private static long initRegionId() {
        // REGION_ID = loadfromDB();
        long regionId = new SecureRandom().nextInt(256);
        if ((regionId & REGION_MASK) == 0) {
            System.err.println("region num must be under 256.");
            throw new IllegalArgumentException("The region num invalid(it must fit in two bytes)");
        }
        System.out.println("region id:" + regionId);
        return regionId;
    }
    
    /**
     * 启动时注册服务节点NodeId，为确保唯一标识，可做静态配置，也可从配置中心或DB中加载
     * @return
     * @author    ghj
     * @since     2020-03-13 00:00:37
     */
    private static long initNodeId() {
        long nodeId = 0;
        NODE_MAP.put("192.168.0.1", 1L);
        NODE_MAP.put("192.168.0.2", 2L);
        String hostip = null;
        try {
            hostip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            System.err.println("unable to get host name.");
        }
        if(NODE_MAP.get(hostip) != null) {
            nodeId = NODE_MAP.get(hostip);
        }
        else {
            System.err.println("node not found in config, use ip hashcode.");
            nodeId = Long.valueOf(hostip.hashCode());
        }
        System.out.println("node id:" + nodeId);
        return nodeId;
    }

    /**
     * 启动时加载持久化的lastTimestamp
     * 本方案是为了处理时钟回拨的情况，避免因时钟回拨导致的timestamp冲突
     * @author    ghj
     * @since     2020-03-13 00:07:47
     */
    private static void initLastTimestamp() {
        try {
            //从持久化存储中加载上次最近更新的时间戳
            // lastTimestampUpdated = loadFromDB();
            lastTimestampUpdated = System.currentTimeMillis() / 1000 - TIME_UPDATE_INTERVAL_IN_SECOND;
        } catch(Exception e) {
            lastTimestampUpdated = System.currentTimeMillis() / 1000 - TIME_UPDATE_INTERVAL_IN_SECOND;
        }
        if (System.currentTimeMillis() <= lastTimestampUpdated + TIME_UPDATE_INTERVAL_IN_SECOND) {
            lastTimestampUpdated += TIME_UPDATE_INTERVAL_IN_SECOND;
            lastTimestamp = lastTimestampUpdated;
            // updateToDB(lastTimestampUpdated);
        }
        System.out.println("lasteTimestamp updated:" + lastTimestampUpdated);
    }
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("------------------正常测试---------------------");
        System.out.println("DistributedID:"+DistributedIdGenerator.nextId());
        
        System.out.println("------------------TPS测试---------------------");
        // I have tested 2000w TPS(1 thread) in my macOS specifications:
        // 2.2 GHz Intel Core i7(4 cores), 256 KB L2(per core), 6 MB L3, 16 GB 1600 MHz DDR3
        long start = System.currentTimeMillis();
        final long ROUNDS = 100000000;
        for(int i=0; i<ROUNDS; i++) {
            DistributedIdGenerator.nextId();
        }
        long end = System.currentTimeMillis();
        long elapsed = (end-start)/1000;
        System.out.println("ID Generator TPS(in 1 thread):"+ROUNDS/(elapsed==0?1:elapsed));
        
        System.out.println("------------------并发测试---------------------");
        // I have tested 1000w+ TPS(100 threads) in my macOS specifications:
        // 2.2 GHz Intel Core i7(4 cores), 256 KB L2(per core), 6 MB L3, 16 GB 1600 MHz DDR3
        start = System.currentTimeMillis();
        final int CONCURRENTS = 100;
        final CountDownLatch startSignal = new CountDownLatch(1); 
        final CountDownLatch doneSignal = new CountDownLatch(CONCURRENTS); 
        for(int i=0; i<CONCURRENTS; i++) {
            new Thread() {
                public void run() {
                    try {
                        startSignal.await();
                        for(int i=0; i<ROUNDS/CONCURRENTS; i++) {
                            DistributedIdGenerator.nextId();
                        }
                        doneSignal.countDown();
                    } catch (InterruptedException e) { }
                }
            }.start();
        }
        startSignal.countDown();
        doneSignal.await();
        end = System.currentTimeMillis();
        elapsed = (end-start)/1000;
        System.out.println("ID Generator TPS(in 100 threads):"+ROUNDS/(elapsed==0?1:elapsed));
    }
}
