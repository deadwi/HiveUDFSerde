package example.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.Text;

public final class SelectOne extends UDAF {
  protected static class State {
    Integer key;
    Double value;
    boolean ascend;
  }

  public static class Evaluator implements UDAFEvaluator {
    private State state;

    public Evaluator() {
      super();
      init();
    }

    @Override
    public void init() {
      state = new State();
      state.key = null;
      state.value = null;
      state.ascend = true;
    }

    protected boolean getAscending() {
      return state.ascend;
    }

    public boolean iterate(Integer o, Double v, boolean isAscend) {
      state.ascend = isAscend;
      if (o != null) {
        if (state.key != null) {
          boolean doInsert = isAscend ? o < state.key : o > state.key;
          if (doInsert) {
            state.key = o;
            state.value = v;
          }
        } else {
          state.key = o;
          state.value = v;
        }
      }
      return true;
    }

    public State terminatePartial() {
      return state.key == null ? null : state;
    }

    public boolean merge(State o) {
      if (o != null) {
        state.ascend = o.ascend;
        iterate(o.key, o.value, o.ascend);
      }
      return true;
    }

    public Double terminate() {
      if (state.key == null)
        return null;
      return state.value;
    }
  }

  protected static class StateS {
    Text key;
    Text value;
    boolean ascend;
  }

  public static class StringEvaluator implements UDAFEvaluator {
    private StateS state;

    public StringEvaluator() {
      super();
      init();
    }

    @Override
    public void init() {
      state = new StateS();
      state.key = null;
      state.value = null;
      state.ascend = true;
    }

    protected boolean getAscending() {
      return state.ascend;
    }

    public boolean iterate(Text o, Text v, boolean isAscend) {
      state.ascend = isAscend;
      if (o != null) {
        if (state.key != null) {
          boolean doInsert = isAscend ? o.compareTo(state.key) < 0 : o.compareTo(state.key) > 0;
          if (doInsert) {
            state.key = new Text(o);
            state.value = new Text(v);
          }
        } else {
          state.key = new Text(o);
          state.value = new Text(v);
        }
      }
      return true;
    }

    public StateS terminatePartial() {
      return state.key == null ? null : state;
    }

    public boolean merge(StateS o) {
      if (o != null) {
        state.ascend = o.ascend;
        iterate(o.key, o.value, o.ascend);
      }
      return true;
    }

    public Text terminate() {
      if (state.key == null)
        return null;
      return state.value;
    }
  }
}
