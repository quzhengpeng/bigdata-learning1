package io.github.quzhengpeng.java.oom;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * VM Args:-verbose:gc -Xms20M -Xmx20M -Xmn10M -Xss256K -XX:MaxDirectMemorySize=10m -XX:SurvivorRatio=8 -XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError
 * 使用 unsafe 分配本机内存
 * 这个 jvm 参数我试验不好使，会直接吧 cpu 和内存占满，然后卡死等着程序自动结束
 */

public class DirectMemoryOOm {

    private static final int _1MB = 2014 * 1024;

    public static void main(String[] args) throws Exception {
        Field unsafeField = Unsafe.class.getDeclaredFields()[0];
        unsafeField.setAccessible(true);
        Unsafe unsafe = (Unsafe) unsafeField.get(null);
        while (true) {
            unsafe.allocateMemory(_1MB);
        }
    }
}
