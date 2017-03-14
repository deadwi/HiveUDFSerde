package example.hive.udf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by jihun.jo on 2017-03-08.
 */
public class MakeEnumTest {
  @Test
  public void evaluateInt() throws Exception {
    MakeEnum instance = new MakeEnum();
    assertEquals(new IntWritable(1), instance.evaluate(new IntWritable(1), new IntWritable(9), new IntWritable(19)));
    assertEquals(new IntWritable(1), instance.evaluate(new IntWritable(9), new IntWritable(9), new IntWritable(19)));
    assertEquals(new IntWritable(2), instance.evaluate(new IntWritable(15), new IntWritable(9), new IntWritable(19)));
    assertEquals(new IntWritable(2), instance.evaluate(new IntWritable(19), new IntWritable(9), new IntWritable(19)));
    assertEquals(new IntWritable(3), instance.evaluate(new IntWritable(20), new IntWritable(9), new IntWritable(19)));
  }

  @Test
  public void evaluateDouble() throws Exception {
    MakeEnum instance = new MakeEnum();
    assertEquals(new IntWritable(1), instance.evaluate(new DoubleWritable(1.5), new DoubleWritable(1.5), new DoubleWritable(1.7)));
    assertEquals(new IntWritable(2), instance.evaluate(new DoubleWritable(1.5000001), new DoubleWritable(1.5), new DoubleWritable(1.7)));
    assertEquals(new IntWritable(2), instance.evaluate(new DoubleWritable(1.7), new DoubleWritable(1.5), new DoubleWritable(1.7)));
    assertEquals(new IntWritable(3), instance.evaluate(new DoubleWritable(1.7000001), new DoubleWritable(1.5), new DoubleWritable(1.7)));
  }

  @Test
  public void evaluateLong() throws Exception {
    MakeEnum instance = new MakeEnum();
    assertEquals(new IntWritable(1), instance.evaluate(new LongWritable(4294967295l), new LongWritable(20000000000l), new LongWritable(40000000000l)));
    assertEquals(new IntWritable(1), instance.evaluate(new LongWritable(20000000000l), new LongWritable(20000000000l), new LongWritable(40000000000l)));
    assertEquals(new IntWritable(2), instance.evaluate(new LongWritable(40000000000l), new LongWritable(20000000000l), new LongWritable(40000000000l)));
    assertEquals(new IntWritable(3), instance.evaluate(new LongWritable(40000000001l), new LongWritable(20000000000l), new LongWritable(40000000000l)));
  }

  @Test
  public void evaluateString() throws Exception {
    MakeEnum instance = new MakeEnum();
    assertEquals(new IntWritable(1), instance.evaluate(new Text("10"),new Text("100"),new Text("200"),new Text("300")));
    assertEquals(new IntWritable(1), instance.evaluate(new Text("100"),new Text("100"),new Text("200"),new Text("300")));
    assertEquals(new IntWritable(2), instance.evaluate(new Text("150"),new Text("100"),new Text("200"),new Text("300")));
    assertEquals(new IntWritable(3), instance.evaluate(new Text("250"),new Text("100"),new Text("200"),new Text("300")));
    assertEquals(new IntWritable(3), instance.evaluate(new Text("300"),new Text("100"),new Text("200"),new Text("300")));
    assertEquals(new IntWritable(4), instance.evaluate(new Text("400"),new Text("100"),new Text("200"),new Text("300")));
  }

}