package io.github.quzhengpeng.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
        name = "hello",
        value = "",
        extended = ""
)

/*
 * ADD jar /home/hive/bigdata-learning-1.0-SNAPSHOT.jar;
 * CREATE TEMPORARY FUNCTION hello AS 'io.github.quzhengpeng.bigdata.hive.udf.Hello';
 */
public class Hello extends UDF {
    public String evaluate(String str) {
        try {
            return "Hello, " + str;
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }
}
