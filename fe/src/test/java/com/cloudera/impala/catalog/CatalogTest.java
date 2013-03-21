// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import static com.cloudera.impala.thrift.ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.impala.catalog.Db.TableLoadingException;
import com.cloudera.impala.common.MetaStoreClientPool;
import com.cloudera.impala.analysis.IntLiteral;
import com.cloudera.impala.analysis.LiteralExpr;
import com.google.common.collect.Sets;

public class CatalogTest {
  private static Catalog catalog;

  @BeforeClass
  public static void setUp() throws Exception {
    catalog = new Catalog();
  }

  @AfterClass
  public static void cleanUp() {
    catalog.close();
  }

  private void checkTableCols(Db db, String tblName, int numClusteringCols,
      String[] colNames, PrimitiveType[] colTypes) throws TableLoadingException {
    Table tbl = db.getTable(tblName);
    assertEquals(tbl.getName(), tblName);
    assertEquals(tbl.getNumClusteringCols(), numClusteringCols);
    List<Column> cols = tbl.getColumns();
    assertEquals(colNames.length, colTypes.length);
    assertEquals(cols.size(), colNames.length);
    Iterator<Column> it = cols.iterator();
    int i = 0;
    while (it.hasNext()) {
      Column col = it.next();
      assertEquals(col.getName(), colNames[i]);
      assertEquals(col.getType(), colTypes[i]);
      ++i;
    }
  }

  private void checkHBaseTableCols(Db db, String hiveTableName, String hbaseTableName,
      String[] hiveColNames, String[] colFamilies, String[] colQualifiers,
      PrimitiveType[] colTypes) throws TableLoadingException{
    checkTableCols(db, hiveTableName, 1, hiveColNames, colTypes);
    HBaseTable tbl = (HBaseTable) db.getTable(hiveTableName);
    assertEquals(tbl.getHBaseTableName(), hbaseTableName);
    List<Column> cols = tbl.getColumns();
    assertEquals(colFamilies.length, colTypes.length);
    assertEquals(colQualifiers.length, colTypes.length);
    Iterator<Column> it = cols.iterator();
    int i = 0;
    while (it.hasNext()) {
      HBaseColumn col = (HBaseColumn)it.next();
      assertEquals(col.getColumnFamily(), colFamilies[i]);
      assertEquals(col.getColumnQualifier(), colQualifiers[i]);
      ++i;
    }
  }

  @Test public void TestColSchema() throws TableLoadingException {
    Db defaultDb = catalog.getDb("functional");
    Db testDb = catalog.getDb("functional_seq");

    assertNotNull(defaultDb);
    assertEquals(defaultDb.getName(), "functional");
    assertNotNull(testDb);
    assertEquals(testDb.getName(), "functional_seq");

    assertNotNull(defaultDb.getTable("alltypes"));
    assertNotNull(defaultDb.getTable("alltypessmall"));
    assertNotNull(defaultDb.getTable("alltypeserror"));
    assertNotNull(defaultDb.getTable("alltypeserrornonulls"));
    assertNotNull(defaultDb.getTable("alltypesagg"));
    assertNotNull(defaultDb.getTable("alltypesaggnonulls"));
    assertNotNull(defaultDb.getTable("alltypesnopart"));
    assertNotNull(defaultDb.getTable("alltypesinsert"));
    assertNotNull(defaultDb.getTable("testtbl"));
    assertNotNull(defaultDb.getTable("dimtbl"));
    assertNotNull(defaultDb.getTable("jointbl"));
    assertNotNull(defaultDb.getTable("liketbl"));
    assertNotNull(defaultDb.getTable("hbasealltypessmall"));
    assertNotNull(defaultDb.getTable("hbasealltypeserror"));
    assertNotNull(defaultDb.getTable("hbasealltypeserrornonulls"));
    assertNotNull(defaultDb.getTable("hbasealltypesagg"));
    assertNotNull(defaultDb.getTable("hbasestringids"));
    assertNotNull(defaultDb.getTable("greptiny"));
    assertNotNull(defaultDb.getTable("rankingssmall"));
    assertNotNull(defaultDb.getTable("uservisitssmall"));

    // IMP-163 - table with string partition column does not load if there are partitions
    assertNotNull(defaultDb.getTable("StringPartitionKey"));

    // functional_seq contains the same tables as functional
    assertNotNull(testDb.getTable("alltypes"));
    assertNotNull(testDb.getTable("testtbl"));

    // Test non-existant table
    assertNull(defaultDb.getTable("nonexistenttable"));

    checkTableCols(defaultDb, "alltypes", 2,
        new String[]
          {"year", "month", "id", "bool_col", "tinyint_col", "smallint_col",
           "int_col", "bigint_col", "float_col", "double_col", "date_string_col",
           "string_col", "timestamp_col"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.INT, PrimitiveType.INT,
           PrimitiveType.BOOLEAN, PrimitiveType.TINYINT, PrimitiveType.SMALLINT,
           PrimitiveType.INT, PrimitiveType.BIGINT, PrimitiveType.FLOAT,
           PrimitiveType.DOUBLE, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.TIMESTAMP});
    checkTableCols(defaultDb, "testtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(testDb, "testtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(defaultDb, "liketbl", 0,
        new String[] {
            "str_col", "match_like_col", "no_match_like_col", "match_regex_col",
            "no_match_regex_col"},
        new PrimitiveType[]
          {PrimitiveType.STRING, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.STRING});
    checkTableCols(defaultDb, "dimtbl", 0,
        new String[] {"id", "name", "zip"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT});
    checkTableCols(defaultDb, "jointbl", 0,
        new String[] {"test_id", "test_name", "test_zip", "alltypes_id"},
        new PrimitiveType[]
          {PrimitiveType.BIGINT, PrimitiveType.STRING, PrimitiveType.INT,
           PrimitiveType.INT});

    checkHBaseTableCols(defaultDb, "hbasealltypessmall", "hbasealltypessmall",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new PrimitiveType[]
          {PrimitiveType.INT,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.TIMESTAMP});

    checkHBaseTableCols(defaultDb, "hbasealltypeserror", "hbasealltypeserror",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new PrimitiveType[]
          {PrimitiveType.INT,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.TIMESTAMP});

    checkHBaseTableCols(defaultDb, "hbasealltypeserrornonulls", "hbasealltypeserrornonulls",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new PrimitiveType[]
          {PrimitiveType.INT,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.TIMESTAMP});

    checkHBaseTableCols(defaultDb, "hbasealltypesagg", "hbasealltypesagg",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new PrimitiveType[]
          {PrimitiveType.INT,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.TIMESTAMP});

    checkHBaseTableCols(defaultDb, "hbasestringids", "hbasealltypesagg",
        new String[]
          {"id", "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new String[]
          {":key", "bools", "floats", "floats", "ints", "ints", "ints", "ints",
           "strings", "strings", "strings"},
        new String[]
          {null, "bool_col", "double_col", "float_col", "bigint_col", "int_col",
           "smallint_col", "tinyint_col", "date_string_col", "string_col", "timestamp_col"},
        new PrimitiveType[]
          {PrimitiveType.STRING,PrimitiveType.BOOLEAN, PrimitiveType.DOUBLE,
           PrimitiveType.FLOAT, PrimitiveType.BIGINT, PrimitiveType.INT,
           PrimitiveType.SMALLINT, PrimitiveType.TINYINT, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.TIMESTAMP});

    checkTableCols(defaultDb, "greptiny", 0,
        new String[]
          {"field"},
        new PrimitiveType[]
          {PrimitiveType.STRING});

    checkTableCols(defaultDb, "rankingssmall", 0,
        new String[]
          {"pagerank", "pageurl", "avgduration"},
        new PrimitiveType[]
          {PrimitiveType.INT, PrimitiveType.STRING, PrimitiveType.INT});

    checkTableCols(defaultDb, "uservisitssmall", 0,
        new String[]
          {"sourceip", "desturl", "visitdate",  "adrevenue", "useragent",
           "ccode", "lcode", "skeyword", "avgtimeonsite"},
        new PrimitiveType[]
          {PrimitiveType.STRING, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.FLOAT, PrimitiveType.STRING, PrimitiveType.STRING,
           PrimitiveType.STRING, PrimitiveType.STRING, PrimitiveType.INT});

    // case-insensitive lookup
    assertEquals(defaultDb.getTable("alltypes"), defaultDb.getTable("AllTypes"));
  }

  @Test public void TestPartitions() throws TableLoadingException {
    HdfsTable table = (HdfsTable) catalog.getDb("functional").getTable("AllTypes");
    List<HdfsPartition> partitions = table.getPartitions();

    // check that partition keys cover the date range 1/1/2009-12/31/2010
    // and that we have one file per partition, plus the default partition
    assertEquals(25, partitions.size());
    Set<Long> months = Sets.newHashSet();
    for (HdfsPartition p: partitions) {
      if (p.getId() == DEFAULT_PARTITION_ID) {
        continue;
      }

      assertEquals(2, p.getPartitionValues().size());

      LiteralExpr key1Expr = p.getPartitionValues().get(0);
      assertTrue(key1Expr instanceof IntLiteral);
      long key1 = ((IntLiteral) key1Expr).getValue();
      assertTrue(key1 == 2009 || key1 == 2010);

      LiteralExpr key2Expr = p.getPartitionValues().get(1);
      assertTrue(key2Expr instanceof IntLiteral);
      long key2 = ((IntLiteral) key2Expr).getValue();
      assertTrue(key2 >= 1 && key2 <= 12);

      months.add(key1 * 100 + key2);

      assertEquals(p.getFileDescriptors().size(), 1);
    }
    assertEquals(months.size(), 24);
  }

  @Test
  public void testStats() throws TableLoadingException {
    // make sure the stats for functional.alltypesagg look correct
    HdfsTable table = (HdfsTable) catalog.getDb("functional").getTable("AllTypesAgg");

    Column idCol = table.getColumn("id");
    assertEquals(idCol.getStats().getAvgSerializedSize(),
        PrimitiveType.INT.getSlotSize(), 0.0001);
    assertEquals(idCol.getStats().getMaxSize(), PrimitiveType.INT.getSlotSize());
    assertTrue(!idCol.getStats().hasNulls());

    Column boolCol = table.getColumn("bool_col");
    assertEquals(boolCol.getStats().getAvgSerializedSize(),
        PrimitiveType.BOOLEAN.getSlotSize(), 0.0001);
    assertEquals(boolCol.getStats().getMaxSize(), PrimitiveType.BOOLEAN.getSlotSize());
    assertTrue(!boolCol.getStats().hasNulls());

    Column tinyintCol = table.getColumn("tinyint_col");
    assertEquals(tinyintCol.getStats().getAvgSerializedSize(),
        PrimitiveType.TINYINT.getSlotSize(), 0.0001);
    assertEquals(tinyintCol.getStats().getMaxSize(), PrimitiveType.TINYINT.getSlotSize());
    assertTrue(tinyintCol.getStats().hasNulls());

    Column smallintCol = table.getColumn("smallint_col");
    assertEquals(smallintCol.getStats().getAvgSerializedSize(),
        PrimitiveType.SMALLINT.getSlotSize(), 0.0001);
    assertEquals(smallintCol.getStats().getMaxSize(),
        PrimitiveType.SMALLINT.getSlotSize());
    assertTrue(smallintCol.getStats().hasNulls());

    Column intCol = table.getColumn("int_col");
    assertEquals(intCol.getStats().getAvgSerializedSize(),
        PrimitiveType.INT.getSlotSize(), 0.0001);
    assertEquals(intCol.getStats().getMaxSize(), PrimitiveType.INT.getSlotSize());
    assertTrue(intCol.getStats().hasNulls());

    Column bigintCol = table.getColumn("bigint_col");
    assertEquals(bigintCol.getStats().getAvgSerializedSize(),
        PrimitiveType.BIGINT.getSlotSize(), 0.0001);
    assertEquals(bigintCol.getStats().getMaxSize(), PrimitiveType.BIGINT.getSlotSize());
    assertTrue(bigintCol.getStats().hasNulls());

    Column floatCol = table.getColumn("float_col");
    assertEquals(floatCol.getStats().getAvgSerializedSize(),
        PrimitiveType.FLOAT.getSlotSize(), 0.0001);
    assertEquals(floatCol.getStats().getMaxSize(), PrimitiveType.FLOAT.getSlotSize());
    assertTrue(floatCol.getStats().hasNulls());

    Column doubleCol = table.getColumn("double_col");
    assertEquals(doubleCol.getStats().getAvgSerializedSize(),
        PrimitiveType.DOUBLE.getSlotSize(), 0.0001);
    assertEquals(doubleCol.getStats().getMaxSize(), PrimitiveType.DOUBLE.getSlotSize());
    assertTrue(doubleCol.getStats().hasNulls());

    Column timestampCol = table.getColumn("timestamp_col");
    assertEquals(timestampCol.getStats().getAvgSerializedSize(),
        PrimitiveType.TIMESTAMP.getSlotSize(), 0.0001);
    assertEquals(timestampCol.getStats().getMaxSize(),
        PrimitiveType.TIMESTAMP.getSlotSize());
    assertTrue(timestampCol.getStats().hasNulls());

    Column stringCol = table.getColumn("string_col");
    assertTrue(
        stringCol.getStats().getAvgSerializedSize() > PrimitiveType.STRING.getSlotSize());
    assertEquals(stringCol.getStats().getMaxSize(), 3);
    assertTrue(!stringCol.getStats().hasNulls());
  }

  @Test
  public void testInternalHBaseTable() throws TableLoadingException {
    // Cast will fail if table not an HBaseTable
    HBaseTable table = 
        (HBaseTable)catalog.getDb("functional").getTable("internal_hbase_table");
    assertNotNull("internal_hbase_table was not found", table);
  }

  @Test(expected = TableLoadingException.class)
  public void testMapColumnsFails() throws TableLoadingException {
    Table table = catalog.getDb("functional").getTable("map_table");
  }

  @Test(expected = TableLoadingException.class)
  public void testArrayColumnsFails() throws TableLoadingException {
    Table table = catalog.getDb("functional").getTable("array_table");
  }

  @Test
  public void testDatabaseDoesNotExist() {
    Db nonExistentDb = catalog.getDb("doesnotexist");
    assertNull(nonExistentDb);
  }

  @Test
  public void testLoadingUnsupportedTableTypes() {
    try {
      Table table = catalog.getDb("functional").getTable("hive_view");
      fail("Expected TableLoadingException when loading VIRTUAL_VIEW");
    } catch (TableLoadingException e) {
      assertEquals("Unsupported table type 'VIRTUAL_VIEW' for: functional.hive_view",
          e.getMessage());
    }

    try {
      Table table = catalog.getDb("functional").getTable("hive_index_tbl");
      fail("Expected TableLoadingException when loading INDEX_TABLE");
    } catch (TableLoadingException e) {
      assertEquals("Unsupported table type 'INDEX_TABLE' for: functional.hive_index_tbl",
          e.getMessage());
    }
  }

  // This table has metadata set so the escape is \n, which is also the tuple delim. This
  // test validates that our representation of the catalog fixes this and removes the
  // escape char.
  @Test public void TestTableWithBadEscapeChar() throws TableLoadingException {
    HdfsTable table =
        (HdfsTable) catalog.getDb("functional").getTable("escapechartesttable");
    List<HdfsPartition> partitions = table.getPartitions();
    for (HdfsPartition p: partitions) {
      HdfsStorageDescriptor desc = p.getInputFormatDescriptor();
      assertEquals(desc.getEscapeChar(), HdfsStorageDescriptor.DEFAULT_ESCAPE_CHAR);
    }
  }

  @Test
  public void TestHiveMetaStoreClientCreationRetry() throws MetaException {
    HiveConf conf = new HiveConf(CatalogTest.class);
    // Set the Metastore warehouse to an empty string to trigger a MetaException
    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, "");
    try {
      MetaStoreClientPool pool = new MetaStoreClientPool(1, conf);
      fail("Expected MetaException");
    } catch (IllegalStateException e) {
      assertTrue(e.getCause() instanceof MetaException);
    }

    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, "/some/valid/path");
    MetaStoreClientPool pool = new MetaStoreClientPool(1, conf);

    // TODO: This doesn't fully validate the retry logic. In the future we
    // could throw an exception when the retry attempts maxed out. This exception
    // would have details on the number of retries, etc. We also need coverage for the
    // case where we we have a few failure/retries and then a success.
  }
}
