package example.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.*;

public final class MakeEnum extends UDF {

  public < T extends WritableComparable<T> > IntWritable doEvaluate(T value, T... range) {
    int i = 1;
    for (T v : range) {
      // value <= v
      if (value.compareTo(v)<=0)
        return new IntWritable(i);
      i++;
    }
    return new IntWritable(i);
  }

  public IntWritable evaluate(IntWritable value, IntWritable... range) {
    return doEvaluate(value, range);
  }

  public IntWritable evaluate(DoubleWritable value, DoubleWritable... range) {
    return doEvaluate(value, range);
  }

  public IntWritable evaluate(LongWritable value, LongWritable... range) {
    return doEvaluate(value, range);
  }

  public IntWritable evaluate(Text value, Text... range) {
    int i = 1;
    for (Text v : range) {
      if (value.compareTo(v)<=0)
        return new IntWritable(i);
      i++;
    }
    return new IntWritable(i);
  }
}
