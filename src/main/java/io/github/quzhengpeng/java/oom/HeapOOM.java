package io.github.quzhengpeng.java.oom;

import java.util.ArrayList;
import java.util.List;

/**
 * VM Args:-verbose:gc -Xms20M -Xmx20M -Xmn10M -XX:SurvivorRatio=8 -XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError
 * Java 堆内存溢出异常测试
 */

public class HeapOOM {

    private static class OOMObject {

    }

    public static void main(String[] args) {

        List<OOMObject> list = new ArrayList<OOMObject>();

        while (true) {
            list.add(new OOMObject());
        }
    }
}
