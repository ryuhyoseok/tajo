/**
 * 
 */
package nta.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nta.catalog.proto.CatalogProtos.ContainFunctionRequest;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.GetAllTableNamesResponse;
import nta.catalog.proto.CatalogProtos.GetFunctionMetaRequest;
import nta.catalog.proto.CatalogProtos.GetFunctionsResponse;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.catalog.proto.CatalogProtos.UnregisterFunctionRequest;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;
import nta.rpc.protocolrecords.PrimitiveProtos.StringProto;

import org.apache.hadoop.conf.Configuration;

/**
 * This class provides a catalog service interface in
 * local.
 * 
 * @author Hyunsik Choi
 *
 */
public class LocalCatalog implements CatalogService {
  private CatalogServer catalog;
  
  public LocalCatalog(final Configuration conf) throws IOException {
    this.catalog = new CatalogServer(conf);
    this.catalog.start();
  }
  
  public LocalCatalog(final CatalogServer server) {
    this.catalog = server;
  }

  @Override
  public final TableDesc getTableDesc(final String name) {
    return TableDesc.Factory.create(catalog.getTableDesc(StringProto.newBuilder().setValue(name).build()));
  }

  @Override
  public final Collection<String> getAllTableNames() {
    List<String> protos = new ArrayList<String>();
    GetAllTableNamesResponse response =
        catalog.getAllTableNames(NullProto.newBuilder().build());
    int size = response.getTableNameCount();
    for (int i = 0; i < size; i++) {
      protos.add(response.getTableName(i));
    }
    return protos;
  }

  @Override
  public final Collection<FunctionDesc> getFunctions() {
    List<FunctionDesc> list = new ArrayList<FunctionDesc>();
    GetFunctionsResponse response =
        catalog.getFunctions(NullProto.newBuilder().build());
    int size = response.getFunctionDescCount();
    for (int i = 0; i < size; i++) {
      list.add(new FunctionDesc(response.getFunctionDesc(i)));
    }
    return list;
  }

  @Override
  public final void addTable(final TableDesc desc) {
    catalog.addTable((TableDescProto) desc.getProto());
  }

  @Override
  public final void deleteTable(final String name) {
    catalog.deleteTable(StringProto.newBuilder().setValue(name).build());
  }

  @Override
  public final boolean existsTable(final String tableId) {
    return catalog
        .existsTable(StringProto.newBuilder().setValue(tableId).build())
        .getValue();
  }

  @Override
  public final void registerFunction(final FunctionDesc funcDesc) {
    catalog.registerFunction(funcDesc.getProto());
  }

  @Override
  public final void unregisterFunction(final String signature, 
      DataType...paramTypes) {
    UnregisterFunctionRequest.Builder builder =
        UnregisterFunctionRequest.newBuilder();
    builder.setSignature(signature);
    int size = paramTypes.length;
    for (int i = 0; i < size; i++) {
      builder.addParameterTypes(paramTypes[i]);
    }
    catalog.unregisterFunction(builder.build());
  }

  @Override
  public final FunctionDesc getFunction(final String signature,
      DataType...paramTypes) {
    GetFunctionMetaRequest.Builder builder =
        GetFunctionMetaRequest.newBuilder();
    builder.setSignature(signature);
    int size = paramTypes.length;
    for (int i = 0; i < size; i++) {
      builder.addParameterTypes(paramTypes[i]);
    }
    return FunctionDesc.create(catalog.getFunctionMeta(builder.build()));
  }

  @Override
  public final boolean containFunction(final String signature, 
      DataType...paramTypes) {
    ContainFunctionRequest.Builder builder =
        ContainFunctionRequest.newBuilder();
    builder.setSignature(signature);
    int size = paramTypes.length;
    for (int i = 0; i < size; i++) {
      builder.addParameterTypes(paramTypes[i]);
    }
    return catalog.containFunction(builder.build()).getValue();
  }

  @Override
  public final List<HostInfo> getHostByTable(final String tableId) {    
    return catalog.getFragmentServingInfo(tableId);
  }
  
  @Override
  public final void updateAllTabletServingInfo(final List<String> onlineServers)
      throws IOException {
    catalog.updateAllFragmentServingInfo(onlineServers);
  }
  
  public CatalogServer getServer() {
	  return this.catalog;
  }
}
