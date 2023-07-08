package io.github.quzhengpeng.java.oom;

import java.util.ArrayList;
import java.util.List;

/*** VM Args:-verbose:gc -Xms20M -Xmx20M -Xmn10M -Xss256K -XX:PermSize=10M -XX:MaxPermSize=10M -XX:MaxMetaspaceSize=10M -XX:SurvivorRatio=8 -XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError

 * 运行时常量池导致的内存溢出异常
 * jdk1.8 之后取消了永久代取而代之的是元空间（MetaSpace）
 * -XX:MetaspaceSize=8m
 * -XX:MinMetaspaceSize=8m
 * -XX:MaxMetaspaceSize=16m
 * 这个例子在 jdk1.8 中是不成功的
 */

public class RuntimeConstantPoolOOM {

    public static void main(String[] args) {
        // 使用 List 保持着常量池引用，避免 Full GC 回收常量池行为
        List<String> list = new ArrayList<String>();
        // 10MB 的 PermSize 在 integer 范围内足够产 OOM 了
        int i = 0;
        while (true) {
            list.add(String.valueOf(i++).intern());
        }
    }
}
