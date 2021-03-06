package tajo.engine.function;

import org.junit.Test;
import tajo.datum.ArrayDatum;
import tajo.datum.DatumFactory;
import tajo.engine.function.builtin.AvgLong;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
public class TestAggFunction {

  @Test
  public void testAvgInt() {
    Tuple [] tuples = new Tuple[5];

    for (int i = 1; i <= 5; i++) {
      tuples[i-1] = new VTuple(1);
      tuples[i-1].put(0, DatumFactory.createInt(i));
    }

    AvgLong avg = new AvgLong();
    FunctionContext ctx = avg.newContext();
    for (int i = 1; i <= 5; i++) {
      avg.eval(ctx, tuples[i-1]);
    }

    assertTrue(15 / 5 == avg.terminate(ctx).asDouble());


    Tuple [] tuples2 = new Tuple[10];

    FunctionContext ctx2 = avg.newContext();
    for (int i = 1; i <= 10; i++) {
      tuples2[i-1] = new VTuple(1);
      tuples2[i-1].put(0, DatumFactory.createInt(i));
      avg.eval(ctx2, tuples2[i-1]);
    }
    assertTrue(55 / 10 == avg.terminate(ctx2).asDouble());


    avg.merge(ctx, new VTuple(((ArrayDatum)avg.getPartialResult(ctx2)).toArray()));
    assertTrue((15 + 55) / (5 + 10) == avg.terminate(ctx).asDouble());
  }
}
