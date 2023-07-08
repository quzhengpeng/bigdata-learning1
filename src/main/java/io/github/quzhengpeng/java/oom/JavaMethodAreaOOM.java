package io.github.quzhengpeng.java.oom;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

/**
 * VM Args:-verbose:gc -Xms20M -Xmx20M -Xmn10M -Xss256K -XX:MaxMetaspaceSize=16m -XX:SurvivorRatio=8 -XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError
 * 借助 CGLib 使得方法区出现内存溢出异常
 * jdk1.8 之后取消了永久代取而代之的是元空间（MetaSpace）
 * -XX:MetaspaceSize=8m
 * -XX:MinMetaspaceSize=8m
 * -XX:MaxMetaspaceSize=16m
 * 这个例子在 jdk1.8 中不会报 OutOfMemoryError： PremGen space 的错误，而会报 OutOfMemoryError：Metaspace 的错误
 */

public class JavaMethodAreaOOM {

    public static void main(String[] args) {
        while (true) {
            Enhancer enhancer = new Enhancer();
            enhancer.setSuperclass(OOMObject.class);
            enhancer.setUseCache(false);
            enhancer.setCallback(new MethodInterceptor() {
                public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
                    return proxy.invokeSuper(obj, args);
                }
            });
            enhancer.create();
        }
    }

    static class OOMObject {

    }
}
