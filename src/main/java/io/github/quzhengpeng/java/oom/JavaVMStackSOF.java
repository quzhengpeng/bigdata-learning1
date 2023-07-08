package io.github.quzhengpeng.java.oom;

/**
 * VM Args:-verbose:gc -Xms20M -Xmx20M -Xmn10M -Xss256K -XX:SurvivorRatio=8 -XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError
 * 虚拟机栈和本地方方法栈 OOM 测试
 */

public class JavaVMStackSOF {

    private int stackLength = 1;

    private void stackLeak() {
        stackLength++;
        stackLeak();
    }

    public static void main(String[] args) {
        JavaVMStackSOF oom = new JavaVMStackSOF();
        try {
            oom.stackLeak();
        } catch (Throwable e) {
            System.out.println("stack length:" + oom.stackLength);
            throw e;
        }
    }
}

