package example.hive.udf;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jihun.jo on 2017-03-08.
 */
public class MakeEnumRepeatDescTest {
  @Test
  public void evaluate() throws Exception {
    MakeEnumRepeatDesc instance = new MakeEnumRepeatDesc();
    assertEquals(new IntWritable(0), instance.evaluate(new IntWritable(22), new IntWritable(28), new IntWritable(7), new IntWritable(10)));
    assertEquals(new IntWritable(0), instance.evaluate(new IntWritable(28), new IntWritable(28), new IntWritable(7), new IntWritable(10)));
    assertEquals(new IntWritable(-1), instance.evaluate(new IntWritable(35), new IntWritable(28), new IntWritable(7), new IntWritable(10)));
    assertEquals(new IntWritable(1), instance.evaluate(new IntWritable(21), new IntWritable(28), new IntWritable(7), new IntWritable(10)));
    assertEquals(new IntWritable(1), instance.evaluate(new IntWritable(15), new IntWritable(28), new IntWritable(7), new IntWritable(10)));
    assertEquals(new IntWritable(2), instance.evaluate(new IntWritable(11), new IntWritable(28), new IntWritable(7), new IntWritable(10)));
    assertEquals(new IntWritable(3), instance.evaluate(new IntWritable(4), new IntWritable(28), new IntWritable(7), new IntWritable(10)));
    assertEquals(new IntWritable(3), instance.evaluate(new IntWritable(1), new IntWritable(28), new IntWritable(7), new IntWritable(10)));
    assertEquals(new IntWritable(2), instance.evaluate(new IntWritable(1), new IntWritable(28), new IntWritable(7), new IntWritable(2)));
  }
}