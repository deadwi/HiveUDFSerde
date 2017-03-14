package example.hive.udaf;

import org.junit.Test;

import static org.junit.Assert.*;
import org.apache.hadoop.io.Text;

/**
 * Created by jihun.jo on 2017-03-09.
 */
public class SelectOneTest {
  @Test
  public void evaluate() throws Exception {
    SelectOne.Evaluator so = new SelectOne.Evaluator();
    so.init();
    so.iterate(60,100.0,false);
    SelectOne.State state = so.terminatePartial();
    assertEquals(new Integer(60), state.key);
    assertEquals(new Double(100.0), state.value);
    assertEquals(false, state.ascend);

    so.init();
    so.iterate(70,200.0,false);
    so.iterate(30,300.0,false);
    so.merge(state);
    assertEquals(new Double(200.0), so.terminate());
  }

  @Test
  public void evaluateString() throws Exception {
    SelectOne.StringEvaluator so = new SelectOne.StringEvaluator();
    so.init();
    so.iterate(new Text("60"), new Text("good"), true);
    SelectOne.StateS state = so.terminatePartial();
    assertEquals(new Text("60"), state.key);
    assertEquals(new Text("good"), state.value);
    assertEquals(true, state.ascend);

    so.init();
    so.iterate(new Text("70"),new Text("bad"),true);
    so.iterate(new Text("30"),new Text("none"),true);
    so.merge(state);
    assertEquals(new Text("none"), so.terminate());
  }
}