/*
 *  Copyright 2012 Database Lab., Korea Univ.
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package tajo;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos;
import tajo.client.ResultSetUtil;
import tajo.storage.CSVFile2;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class TestSelectQuery {
  private static TpchTestBase tpch;
  public TestSelectQuery() throws IOException {
    super();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = new TpchTestBase();
    tpch.setUp();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tpch.tearDown();
  }
  
  @Test
  public final void testSelect() throws Exception {
    ResultSet res = tpch.execute("select l_orderkey, l_partkey from lineitem");
    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(1, res.getInt(2));

    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(1, res.getInt(2));

    res.next();
    assertEquals(2, res.getInt(1));
    assertEquals(2, res.getInt(2));
  }

  @Test
  public final void testSelect2() throws Exception {
    ResultSet res = tpch.execute("select l_orderkey, l_partkey, l_orderkey + l_partkey as plus from lineitem");
    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(1, res.getInt(2));
    assertEquals(2, res.getInt(3));

    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(1, res.getInt(2));
    assertEquals(2, res.getInt(3));

    res.next();
    assertEquals(2, res.getInt(1));
    assertEquals(2, res.getInt(2));
    assertEquals(4, res.getInt(3));
  }

  @Test
  public final void testSelect3() throws Exception {
    ResultSet res = tpch.execute("select l_orderkey + l_partkey as plus from lineitem");
    res.next();
    assertEquals(2, res.getInt(1));

    res.next();
    assertEquals(2, res.getInt(1));

    res.next();
    assertEquals(4, res.getInt(1));
  }

  @Test
  public final void testSelectAsterik() throws Exception {
    ResultSet res = tpch.execute("select * from lineitem");
    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(1, res.getInt(2));
    assertEquals(7706, res.getInt(3));
    assertEquals(1, res.getInt(4));
    assertTrue(17 == res.getFloat(5));
    assertTrue(21168.23f == res.getFloat(6));
    assertTrue(0.04f == res.getFloat(7));
    assertTrue(0.02f == res.getFloat(8));
    assertEquals("N",res.getString(9));
    assertEquals("O",res.getString(10));
    assertEquals("1996-03-13",res.getString(11));
    assertEquals("1996-02-12",res.getString(12));
    assertEquals("1996-03-22",res.getString(13));
    assertEquals("DELIVER IN PERSON",res.getString(14));
    assertEquals("TRUCK",res.getString(15));
    assertEquals("egular courts above the",res.getString(16));

    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals(1, res.getInt(2));
    assertEquals(7311, res.getInt(3));
    assertEquals(2, res.getInt(4));
    assertTrue(36 == res.getFloat(5));
    assertTrue(45983.16f == res.getFloat(6));
    assertTrue(0.09f == res.getFloat(7));
    assertTrue(0.06f == res.getFloat(8));
    assertEquals("N",res.getString(9));
    assertEquals("O",res.getString(10));
    assertEquals("1996-04-12",res.getString(11));
    assertEquals("1996-02-28",res.getString(12));
    assertEquals("1996-04-20",res.getString(13));
    assertEquals("TAKE BACK RETURN",res.getString(14));
    assertEquals("MAIL",res.getString(15));
    assertEquals("ly final dependencies: slyly bold",res.getString(16));
  }

  //@Test
  public final void testSelectDistinct() throws Exception {
    Set<String> result1 = Sets.newHashSet();
    result1.add("1,1");
    result1.add("1,2");
    result1.add("2,1");
    result1.add("3,1");
    result1.add("3,2");

    ResultSet res = tpch.execute(
        "select distinct l_orderkey, l_linenumber from lineitem");
    int cnt = 0;
    while(res.next()) {
      assertTrue(result1.contains(res.getInt(1) + "," + res.getInt(2)));
      cnt++;
    }
    assertEquals(5, cnt);

    res = tpch.execute("select distinct l_orderkey from lineitem");
    Set<Integer> result2 = Sets.newHashSet(1,2,3);
    cnt = 0;
    while (res.next()) {
      assertTrue(result2.contains(res.getInt(1)));
      cnt++;
    }
    assertEquals(3,cnt);
  }

  @Test
  public final void testLikeClause() throws Exception {
    Set<String> result = Sets.newHashSet(
        "ALGERIA", "ETHIOPIA", "INDIA", "INDONESIA", "ROMANIA", "SAUDI ARABIA", "RUSSIA");

    ResultSet res = tpch.execute(
        "select n_name from nation where n_name like '%IA'");
    int cnt = 0;
    while(res.next()) {
      assertTrue(result.contains(res.getString(1)));
      cnt++;
    }
    assertEquals(result.size(), cnt);
  }

  @Test
  public final void testStringCompare() throws Exception {
    Set<Integer> result = Sets.newHashSet(1, 3);

    ResultSet res = tpch.execute(
        "select l_orderkey from lineitem where l_shipdate <= '1996-03-22'");
    int cnt = 0;
    while(res.next()) {
      assertTrue(result.contains(res.getInt(1)));
      cnt++;
    }
    assertEquals(3, cnt);
  }

  @Test
  public final void testRealValueCompare() throws Exception {
    ResultSet res = tpch.execute("select ps_supplycost from partsupp where ps_supplycost = 771.64");

    res.next();
    assertTrue(771.64f == res.getFloat(1));
    assertFalse(res.next());
  }

  @Test
  public final void testCaseWhen() throws Exception {
    ResultSet res = tpch.execute(
        "select r_regionkey, " +
            "case when r_regionkey = 1 then 'one' " +
            "when r_regionkey = 2 then 'two' " +
            "when r_regionkey = 3 then 'three' " +
            "when r_regionkey = 4 then 'four' " +
            "else 'zero' " +
            "end as cond from region");

    Map<Integer, String> result = Maps.newHashMap();
    result.put(0, "zero");
    result.put(1, "one");
    result.put(2, "two");
    result.put(3, "three");
    result.put(4, "four");
    int cnt = 0;
    while(res.next()) {
      assertEquals(result.get(res.getInt(1)), res.getString(2));
      cnt++;
    }

    assertEquals(5, cnt);
  }

  @Test
  public final void testCaseWhenWithoutElse() throws Exception {
    ResultSet res = tpch.execute("select r_regionkey, " +
        "case when r_regionkey = 1 then 'one' " +
        "when r_regionkey = 2 then 'two' " +
        "when r_regionkey = 3 then 'three' " +
        "when r_regionkey = 4 then 'four' " +
        "end as cond from region");

    Map<Integer, String> result = Maps.newHashMap();
    result.put(0, "NULL");
    result.put(1, "one");
    result.put(2, "two");
    result.put(3, "three");
    result.put(4, "four");
    int cnt = 0;
    while(res.next()) {
      assertEquals(result.get(res.getInt(1)), res.getString(2));
      cnt++;
    }

    assertEquals(5, cnt);
  }

  // TODO - to be changed
  public final void testIsNull() throws Exception {
    String [] table = new String[] {"nulltable"};
    Schema schema = new Schema();
    schema.addColumn("col1", CatalogProtos.DataType.INT);
    schema.addColumn("col2", CatalogProtos.DataType.STRING);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
      "1|filled|",
      "2||",
      "3|filled|"
    };
    ResultSet res = TajoTestingUtility
        .run(table, schemas, new Options(), new String[][]{data},
            "select * from nulltable where col2 is null");
    assertTrue(res.next());
    assertEquals(2, res.getInt(1));
    assertFalse(res.next());
  }

  // TODO - to be changed
  public final void testIsNotNull() throws Exception {
    String [] table = new String[] {"nulltable"};
    Schema schema = new Schema();
    schema.addColumn("col1", CatalogProtos.DataType.INT);
    schema.addColumn("col2", CatalogProtos.DataType.STRING);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "1|filled|",
        "2||",
        "3|filled|"
    };
    ResultSet res = TajoTestingUtility
        .run(table, schemas, new Options(), new String[][]{data},
            "select * from nulltable where col2 is not null");
    assertTrue(res.next());
    assertEquals(1, res.getInt(1));
    assertTrue(res.next());
    assertEquals(3, res.getInt(1));
    assertFalse(res.next());
  }

  // TODO - to be changed
  public final void testIsNotNull2() throws Exception {
    String [] table = new String[] {"nulltable"};
    Schema schema = new Schema();
    schema.addColumn("col1", CatalogProtos.DataType.LONG);
    schema.addColumn("col2", CatalogProtos.DataType.LONG);
    schema.addColumn("col3", CatalogProtos.DataType.LONG);
    schema.addColumn("col4", CatalogProtos.DataType.LONG);
    schema.addColumn("col5", CatalogProtos.DataType.LONG);
    schema.addColumn("col6", CatalogProtos.DataType.LONG);
    schema.addColumn("col7", CatalogProtos.DataType.LONG);
    schema.addColumn("col8", CatalogProtos.DataType.LONG);
    schema.addColumn("col9", CatalogProtos.DataType.LONG);
    schema.addColumn("col10", CatalogProtos.DataType.LONG);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        ",,,,672287821,1301460,1,313895860387,126288907,1024",
        ",,,43578,19,13,6,3581,2557,1024"
    };
    Options opts = new Options();
    opts.put(CSVFile2.DELIMITER, ",");
    ResultSet res = TajoTestingUtility
        .run(table, schemas, opts, new String[][]{data},
            "select * from nulltable where col1 is null and col2 is null and col3 is null and col4 = 43578");
    assertTrue(res.next());
    assertEquals(43578, res.getLong(4));
    assertFalse(res.next());
  }

  @Test
  public final void testNotEqual() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey from lineitem where l_orderkey != 1");
    assertTrue(res.next());
    assertEquals(2, res.getInt(1));
    assertTrue(res.next());
    assertEquals(3, res.getInt(1));
    assertTrue(res.next());
    assertEquals(3, res.getInt(1));
    assertFalse(res.next());
  }

  @Test
  public final void testUnion1() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey as num from lineitem union select c_custkey as num from customer");
    int count = 0;
    for (;res.next();) {
      count++;
    }
    assertEquals(8, count);
  }

  @Test
  public final void testUnion2() throws Exception {
    ResultSet res = tpch.execute(
        "select l_orderkey from lineitem as l1 union select l_orderkey from lineitem as l2");
    int count = 0;
    for (;res.next();) {
      count++;
    }
    assertEquals(10, count);
  }
}