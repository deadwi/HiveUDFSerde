package example.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

public final class MakeEnumRepeatDesc extends UDF
{
  public IntWritable evaluate(IntWritable value, IntWritable init, IntWritable gap, IntWritable maxGap)
  {
    int div = (init.get() - value.get()) / gap.get();
    if (div > maxGap.get())
      div = maxGap.get();
    return new IntWritable(div);
  }
}