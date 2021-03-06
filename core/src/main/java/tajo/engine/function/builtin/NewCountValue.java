package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.proto.CatalogProtos;
import tajo.datum.DatumType;
import tajo.engine.function.FunctionContext;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public final class NewCountValue extends NewCountRows {

  public NewCountValue() {
    super(new Column[] {
        new Column("col", CatalogProtos.DataType.ANY)
    });
  }
  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    if (params.get(0).type() != DatumType.NULL) {
      ((CountRowContext) ctx).count++;
    }
  }
}
