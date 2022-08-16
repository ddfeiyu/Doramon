package com.simon.ocean.一致性哈希算法;


import java.nio.charset.Charset;

import java.util.ArrayList;
import java.util.List;

import java.util.SortedMap;

import java.util.TreeMap;

import com.google.common.hash.HashFunction;

import com.google.common.hash.Hashing;

/**
 * 一致性哈希算法java实现
 */
public class ConsistentHash {

    private SortedMap<Long,String> ketamaNodes = new TreeMap<Long,String>();

    private int numberOfReplicas = 1024;

    // 这里使用了谷歌的jar包 -- guava-18.0.jar
    private HashFunction hashFunction = Hashing.md5();

    private List<String> nodes;

    private volatile boolean init = false; //标志是否初始化完成

    public static void main(String[] args) {
        List<String> nodes = new ArrayList<>();
        nodes.add("A");
        nodes.add("B");
        ConsistentHash consistentHash = new ConsistentHash(10, nodes);

        String server_1 = consistentHash.getNodeByKey("1");
        String server_2 = consistentHash.getNodeByKey("2");
        String server_3 = consistentHash.getNodeByKey("3");
        String server_4 = consistentHash.getNodeByKey("4");

        System.out.println("1的找到最近的一个节点： "+server_1);
        System.out.println("2的找到最近的一个节点： "+server_2);
        System.out.println("3的找到最近的一个节点： "+server_3);
        System.out.println("4的找到最近的一个节点： "+server_4);

        consistentHash.addNode("C");
        System.out.println("----------------------添加节点C ");

        server_1 = consistentHash.getNodeByKey("1");
        server_2 = consistentHash.getNodeByKey("2");
        server_3 = consistentHash.getNodeByKey("3");
         server_4 = consistentHash.getNodeByKey("4");
        System.out.println("1的找到最近的一个节点： "+server_1);
        System.out.println("2的找到最近的一个节点： "+server_2);
        System.out.println("3的找到最近的一个节点： "+server_3);
        System.out.println("4的找到最近的一个节点： "+server_4);

        consistentHash.removeNode("C");
        System.out.println("----------------------移除节点C ");

        server_1 = consistentHash.getNodeByKey("1");
        server_2 = consistentHash.getNodeByKey("2");
        server_3 = consistentHash.getNodeByKey("3");
        server_4 = consistentHash.getNodeByKey("4");
        System.out.println("1的找到最近的一个节点： "+server_1);
        System.out.println("2的找到最近的一个节点： "+server_2);
        System.out.println("3的找到最近的一个节点： "+server_3);
        System.out.println("4的找到最近的一个节点： "+server_4);

        /**
         * 打印日志：
         *
         * 1的找到最近的一个节点： B
         * 2的找到最近的一个节点： B
         * 3的找到最近的一个节点： A
         * 4的找到最近的一个节点： A
         * ----------------------添加节点C
         * 1的找到最近的一个节点： C
         * 2的找到最近的一个节点： B
         * 3的找到最近的一个节点： A
         * 4的找到最近的一个节点： A
         * ----------------------移除节点C
         * 1的找到最近的一个节点： B
         * 2的找到最近的一个节点： B
         * 3的找到最近的一个节点： A
         * 4的找到最近的一个节点： A
         */
    }

    // 有参数构造函数
    public ConsistentHash(int numberOfReplicas,List<String> nodes){
        this.numberOfReplicas = numberOfReplicas;
        this.nodes = nodes;
        init();
    }

    /**
     * 根据key的哈希值，找到最近的一个节点（服务器）
     * @param key
     * @return
     */
    public String getNodeByKey(String key){
        if(!init){
            throw new RuntimeException("init uncomplete...");
        }
        // 注意，这里是NIO包 java.nio.charset.Charset
        byte[] digest = hashFunction.hashString(key, Charset.forName("UTF-8")).asBytes();
        long hash = hash(digest,0);
        //如果找到这个节点，直接取节点，返回
        if(!ketamaNodes.containsKey(hash)){
            //得到大于当前key的那个子Map，然后从中取出第一个key，就是大于且离它最近的那个key
            SortedMap<Long,String> tailMap = ketamaNodes.tailMap(hash);
            if(tailMap.isEmpty()){
                hash = ketamaNodes.firstKey();
            }else{
                hash = tailMap.firstKey();
            }
        }
        return ketamaNodes.get(hash);
    }

    /**
     * 新增节点
     * @param node
     */
    public synchronized void addNode(String node){
        init = false;
        nodes.add(node);
        init();
    }

    /**
     * 移除节点
     * @param node
     */
    public synchronized void removeNode(String node){
        init = false;
        nodes.remove(node);
        init();
    }


    private void init(){
        ketamaNodes.clear();
        //对所有节点，生成numberOfReplicas个虚拟节点
        for(String node:nodes){
            //每四个虚拟节点为1组
            for(int i=0;i<numberOfReplicas/4;i++){
                //为这组虚拟结点得到惟一名称
                byte[] digest = hashFunction.hashString(node+i, Charset.forName("UTF-8")).asBytes();
                //Md5是一个16字节长度的数组，将16字节的数组每四个字节一组，分别对应一个虚拟结点，这就是为什么上面把虚拟结点四个划分一组的原因
                for(int h=0;h<4;h++){
                    Long k = hash(digest,h);
                    ketamaNodes.put(k,node);
                }
            }
        }
        init = true;
    }


    public void printNodes(){
        for(Long key:ketamaNodes.keySet()){
            System.out.println(ketamaNodes.get(key));
        }
    }

    /**
     * 哈希算法
     * @param digest
     * @param nTime
     * @return
     */
    public static long hash(byte[] digest, int nTime)
    {
        long rv = ((long)(digest[3 + nTime * 4] & 0xFF) << 24)
                | ((long)(digest[2 + nTime * 4] & 0xFF) << 16)
                | ((long)(digest[1 + nTime * 4] & 0xFF) << 8)
                | ((long)digest[0 + nTime * 4] & 0xFF);
        return rv;
    }

}
