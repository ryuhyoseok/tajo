package tajo.engine.function;

import org.junit.Test;
import tajo.datum.StringDatum;
import tajo.storage.Tuple;
import tajo.storage.VTuple;
import static org.junit.Assert.*;

public class TestGeoFunctions {

  @Test
  public void test() {
    StringDatum addr = new StringDatum("163.152.161.190");
    Tuple tuple = new VTuple(new StringDatum[]{addr});
    Country country = new Country();
    assertEquals("KR", country.eval(tuple).asChars());
    InCountry inCountry = new InCountry();

    tuple = new VTuple(2);
    tuple.put(0, addr);
    tuple.put(1, new StringDatum("KR"));
    assertTrue(inCountry.eval(tuple).asBool());
    tuple.put(1, new StringDatum("US"));
    assertFalse(inCountry.eval(tuple).asBool());
  }
}
