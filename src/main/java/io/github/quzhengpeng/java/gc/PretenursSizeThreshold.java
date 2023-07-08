package io.github.quzhengpeng.java.gc;

/**
 * VM Args:-verbose:gc -Xms20M -Xmx20M -Xmn10M -XX:SurvivorRatio=8 -XX:+PrintGCDetails -XX:PretenureSizeThreshold=3145728
 * 大对象直接进入老年代
 * 我在 jdk1.8 下试验，对象确实是进入了老年代，可是年轻代也超过了 4M 内存。
 */
public class PretenursSizeThreshold {

    private static final int _1MB = 1024 * 1024;

    public static void testPretenursSizeThreshold() {
        byte[] allocation;
        allocation = new byte[4 * _1MB];
    }

    public static void main(String[] args) {
        testPretenursSizeThreshold();
    }
}
