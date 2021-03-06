package tajo.catalog;

import tajo.catalog.proto.CatalogProtos.DataType;

import java.util.Collection;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public interface CatalogService {
  
  /**
   * Get a table description by name
   * @param name table name
   * @return a table description
   * @see TableDescImpl
   * @throws Throwable
   */
  TableDesc getTableDesc(String name);
  
  /**
   * 
   * @return
   * @throws tajo.catalog.exception.CatalogException
   */
  Collection<String> getAllTableNames();
  
  /**
   * 
   * @return
   * @throws tajo.catalog.exception.CatalogException
   */
  Collection<FunctionDesc> getFunctions();
  
  /**
   * Add a table via table description
   * @param meta table meta
   * @see TableDescImpl
   * @throws Throwable
   */
  void addTable(TableDesc desc);
  
  /**
   * Drop a table by name
   * @param name table name
   * @throws Throwable
   */
  void deleteTable(String name);
  
  boolean existsTable(String tableId);
  
  void addIndex(IndexDesc index);
  
  boolean existIndex(String indexName);
  
  boolean existIndex(String tableName, String columnName);
  
  IndexDesc getIndex(String indexName);
  
  IndexDesc getIndex(String tableName, String columnName);
  
  void deleteIndex(String indexName);
  
  void registerFunction(FunctionDesc funcDesc);
 
  void unregisterFunction(String signature, DataType...paramTypes);
  
  /**
   * 
   * @param signature
   * @return
   */
  FunctionDesc getFunction(String signature, DataType...paramTypes);
  
  /**
   * 
   * @param signature
   * @return
   */
  boolean containFunction(String signature, DataType...paramTypes);
}