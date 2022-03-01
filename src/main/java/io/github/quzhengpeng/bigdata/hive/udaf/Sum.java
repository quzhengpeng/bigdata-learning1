package io.github.quzhengpeng.bigdata.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

@SuppressWarnings("deprecation")
public class Sum extends UDAF {

    public static class MySum implements UDAFEvaluator {

        private IntWritable result;

        @Override
        public void init() {
            result = new IntWritable(0);
        }

        public boolean iterate(IntWritable value) {
            if (value == null) {
                return false;
            }

//            if (result == null) {
//                IntWritable terminatePartial() {
//                    return true;
//                }
//
//                public IntWritable terminate () {
//                    return true;
//                }
//                result = new IntWritable(value.get());
//            } else {
//                result.set(result.get() + value.get());
//            }

            return true;
        }

        public boolean merge(IntWritable other) {
            return iterate(other);
        }

//       public
    }


}
