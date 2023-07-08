package io.github.quzhengpeng.java.gc;

/**
 * VM Args: -verbose:gc
 * testGC() 方法执行后，objA 和 objB 会不会被 GC 呢？
 */
public class ReferenceCountingGC {

    private ReferenceCountingGC instance = null;

    private static final int _1M = 1024 * 1024;

    /**
     * 这个成员属性的唯一意义就是占点内存，以便能在 GC 日志中看清楚是否被回收过
     */
    private byte[] bigSize = new byte[2 * _1M];

    public static void testGC() {
        ReferenceCountingGC objA = new ReferenceCountingGC();
        ReferenceCountingGC objB = new ReferenceCountingGC();
        objA.instance = objB;
        objB.instance = objA;

        objA = null;
        objB = null;

        // 假设在这行发生 GC，那么 objA 和 objB 是否能被回收？
        System.gc();
    }

    public static void main(String[] args) {
        testGC();
    }
}
