/*
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

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos;
import tajo.client.TajoClient;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.zookeeper.MiniZooKeeperCluster;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.UUID;

public class TajoTestingUtility {
	private static Log LOG = LogFactory.getLog(TajoTestingUtility.class);
	private TajoConf conf;

	/**
	 * Set if we were passed a zkCluster.  If so, we won't shutdown zk as
	 * part of general shutdown.
	 */
	private boolean passedZkCluster = false;
	
	private FileSystem defaultFS = null;
	private MiniDFSCluster dfsCluster;
	private MiniZooKeeperCluster zkCluster = null;
	private MiniTajoCluster tajoCluster;
	private MiniCatalogServer catalogCluster;

	// If non-null, then already a cluster running.
	private File clusterTestBuildDir = null;

	/**
	 * System property key to get test directory value.
	 * Name is as it is because mini dfs has hard-codings to put test data here.
	 */
	public static final String TEST_DIRECTORY_KEY = "test.build.data";

	/**
	 * Default parent directory for test output.
	 */
	public static final String DEFAULT_TEST_DIRECTORY = "target/test-data";

	public TajoTestingUtility() {
		this.conf = new TajoConf();
	}

	public TajoConf getConfiguration() {
		return this.conf;
	}
	
	public void initTestDir() {
		if (System.getProperty(TEST_DIRECTORY_KEY) == null) {
			clusterTestBuildDir = setupClusterTestBuildDir();
			System.setProperty(TEST_DIRECTORY_KEY, clusterTestBuildDir.getAbsolutePath());
		}
	}

	/**
	 * @return Where to write test data on local filesystem; usually
	 * {@link #DEFAULT_TEST_DIRECTORY}
	 * @see #setupClusterTestBuildDir()
	 */
	public static File getTestDir() {
		return new File(System.getProperty(TEST_DIRECTORY_KEY,
			DEFAULT_TEST_DIRECTORY));
	}

	/**
	 * @param subdirName
	 * @return Path to a subdirectory named <code>subdirName</code> under
	 * {@link #getTestDir()}.
	 * @see #setupClusterTestBuildDir()
	 */
	public static File getTestDir(final String subdirName) {
		return new File(getTestDir(), subdirName);
  }

	public File setupClusterTestBuildDir() {
		String randomStr = UUID.randomUUID().toString();
		String dirStr = getTestDir(randomStr).toString();
		File dir = new File(dirStr).getAbsoluteFile();
		// Have it cleaned up on exit
		dir.deleteOnExit();
		return dir;
	}

  ////////////////////////////////////////////////////////
  // HDFS Section
  ////////////////////////////////////////////////////////
  /**
   * Start a minidfscluster.
   * @param servers How many DNs to start.
   * @throws Exception
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers) throws Exception {
    return startMiniDFSCluster(servers, null, null);
  }

  /**
   * Start a minidfscluster.
   * Can only create one.
   * @param servers How many DNs to start.
   * @param dir Where to home your dfs cluster.
   * @param hosts hostnames DNs to run on.
   * @throws Exception
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   * @throws IOException
   */
  public MiniDFSCluster startMiniDFSCluster(int servers,
                                            final File dir, final String hosts[]) throws IOException {
    // This does the following to home the minidfscluster
    // base_dir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/");
    // Some tests also do this:
    //  System.getProperty("test.cache.data", "build/test/cache");
    if (dir == null) {
      this.clusterTestBuildDir = setupClusterTestBuildDir();
    } else {
      this.clusterTestBuildDir = dir;
    }

    System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestBuildDir.toString());
    System.setProperty("test.cache.data", this.clusterTestBuildDir.toString());
    this.dfsCluster = new MiniDFSCluster(0, this.conf, servers, true, true,
        true, null, null, hosts, null);
    // Set this just-started cluser as our filesystem.
    this.defaultFS = this.dfsCluster.getFileSystem();
    this.conf.set("fs.defaultFS", defaultFS.getUri().toString());
    // Do old style too just to be safe.
    this.conf.set("fs.default.name", defaultFS.getUri().toString());

    return this.dfsCluster;
  }

  public void shutdownMiniDFSCluster() throws Exception {
    if (this.dfsCluster != null) {
      // The below throws an exception per dn, AsynchronousCloseException.
      this.dfsCluster.shutdown();
    }
  }

  public boolean isRunningDFSCluster() {
    return this.defaultFS != null;
  }

  public MiniDFSCluster getMiniDFSCluster() {
    return this.dfsCluster;
  }

  public FileSystem getDefaultFileSystem() {
    return this.defaultFS;
  }

  ////////////////////////////////////////////////////////
  // Zookeeper Section
  ////////////////////////////////////////////////////////
  public MiniZooKeeperCluster startMiniZKCluster() throws Exception {
    return startMiniZKCluster(1);
  }

  public MiniZooKeeperCluster startMiniZKCluster(int zookeeperServerNum)
      throws Exception {
    return startMiniZKCluster(setupClusterTestBuildDir(), zookeeperServerNum);
  }

  private MiniZooKeeperCluster startMiniZKCluster(final File dir)
      throws Exception {
    return startMiniZKCluster(dir, 1);
  }

  private MiniZooKeeperCluster startMiniZKCluster(final File dir,
                                                  int zookeeperServerNum) throws Exception {
    this.passedZkCluster = false;
    if(this.zkCluster != null) {
      throw new IOException("Zoookeeper Cluster already running at "+dir);
    }
    this.zkCluster = new MiniZooKeeperCluster();
    int clientPort = this.zkCluster.startup(dir, zookeeperServerNum);
    this.conf.setVar(ConfVars.ZOOKEEPER_ADDRESS, "127.0.0.1:" + clientPort);

    return this.zkCluster;
  }

  public MiniZooKeeperCluster getZKCluster() {
    return this.zkCluster;
  }

  public void shutdownMiniZKCluster() throws IOException {
    if(this.zkCluster != null) {
      this.zkCluster.shutdown();
      this.zkCluster = null;
    }
  }

  ////////////////////////////////////////////////////////
  // Catalog Section
  ////////////////////////////////////////////////////////
  public MiniCatalogServer startCatalogCluster() throws Exception {
    TajoConf c = getConfiguration();
    c.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");

    if(clusterTestBuildDir == null) {
      clusterTestBuildDir = setupClusterTestBuildDir();
    }
    String testDir = clusterTestBuildDir.getAbsolutePath();

    conf.set(TConstants.JDBC_URI,
        "jdbc:derby:"+testDir+"/db");
    LOG.info("derby repository is set to "+conf.get(TConstants.JDBC_URI));

    this.catalogCluster = new MiniCatalogServer(conf);
    CatalogServer catServer = this.catalogCluster.getCatalogServer();
    InetSocketAddress sockAddr = catServer.getBindAddress();
    c.setVar(ConfVars.CATALOG_ADDRESS,
        sockAddr.getHostName() + ":" + sockAddr.getPort());

    return this.catalogCluster;
  }

  public void shutdownCatalogCluster() {
    this.catalogCluster.shutdown();
  }

  public MiniCatalogServer getMiniCatalogCluster() {
    return this.catalogCluster;
  }

  ////////////////////////////////////////////////////////
  // Tajo Cluster Section
  ////////////////////////////////////////////////////////
  private MiniTajoCluster startMiniTajoCluster(File testBuildDir,
                                               final int numSlaves,
                                               boolean local) throws Exception {
    TajoConf c = getConfiguration();
    c.setVar(ConfVars.MASTER_ADDRESS, "localhost:0");
    c.setVar(ConfVars.CATALOG_ADDRESS, "localhost:0");
    conf.set(TConstants.JDBC_URI,
        "jdbc:derby:"+clusterTestBuildDir.getAbsolutePath()+"/db");
    LOG.info("derby repository is set to "+conf.get(TConstants.JDBC_URI));
    if (!local) {
      c.setVar(ConfVars.ENGINE_BASE_DIR,
          getMiniDFSCluster().getFileSystem().getUri() + "/tajo");
    } else {
      c.setVar(ConfVars.ENGINE_BASE_DIR,
          clusterTestBuildDir.getAbsolutePath() + "/tajo");
    }
    c.setVar(ConfVars.WORKER_BASE_DIR,
        clusterTestBuildDir + "/worker");
    c.setVar(ConfVars.WORKER_TMP_DIR,
        clusterTestBuildDir + "/worker/tmp");
    this.tajoCluster = new MiniTajoCluster(c, numSlaves);

    this.conf.setVar(ConfVars.MASTER_ADDRESS,
        c.getVar(ConfVars.MASTER_ADDRESS));
    this.conf.setVar(ConfVars.CATALOG_ADDRESS,
        c.getVar(ConfVars.CATALOG_ADDRESS));

    LOG.info("Mini Tajo cluster is up");
    return this.tajoCluster;
  }

  public void restartTajoCluster(int numSlaves) throws Exception {
    this.tajoCluster.shutdown();
    this.tajoCluster =
        new MiniTajoCluster(new TajoConf(this.conf), numSlaves);

    LOG.info("Minicluster has been restarted");
  }

  public MiniTajoCluster getMiniTajoCluster() {
    return this.tajoCluster;
  }

  public void shutdownMiniTajoCluster() {
    if(this.tajoCluster != null) {
      this.tajoCluster.shutdown();
      this.tajoCluster.join();
    }
    this.tajoCluster = null;
  }

  ////////////////////////////////////////////////////////
  // Meta Cluster Section
  ////////////////////////////////////////////////////////
  /**
   * @throws IOException If a cluster -- dfs or engine -- already running.
   */
  void isRunningCluster(String passedBuildPath) throws IOException {
    if (this.clusterTestBuildDir == null || passedBuildPath != null) return;
    throw new IOException("Cluster already running at " +
        this.clusterTestBuildDir);
  }

  /**
   * This method starts up a tajo cluster with a given number of clusters in
   * distributed mode.
   *
   * @param numSlaves the number of tajo cluster to start up
   * @return a mini tajo cluster
   * @throws Exception
   */
  public MiniTajoCluster startMiniCluster(final int numSlaves)
      throws Exception {
    return startMiniCluster(numSlaves, null);
  }

  public MiniTajoCluster startMiniCluster(final int numSlaves,
                                          final String [] dataNodeHosts) throws Exception {
    // the conf is set to the distributed mode.
    this.conf.setBoolVar(ConfVars.CLUSTER_DISTRIBUTED, true);

    int numDataNodes = numSlaves;
    if(dataNodeHosts != null && dataNodeHosts.length != 0) {
      numDataNodes = dataNodeHosts.length;
    }

    LOG.info("Starting up minicluster with 1 master(s) and " +
        numSlaves + " worker(s) and " + numDataNodes + " datanode(s)");

    // If we already put up a cluster, fail.
    String testBuildPath = conf.get(TEST_DIRECTORY_KEY, null);
    isRunningCluster(testBuildPath);
    if (testBuildPath != null) {
      LOG.info("Using passed path: " + testBuildPath);
    }

    // Make a new random dir to home everything in.  Set it as system property.
    // minidfs reads home from system property.
    this.clusterTestBuildDir = testBuildPath == null?
        setupClusterTestBuildDir() : new File(testBuildPath);

    System.setProperty(TEST_DIRECTORY_KEY,
        this.clusterTestBuildDir.getAbsolutePath());

    startMiniDFSCluster(numDataNodes, this.clusterTestBuildDir, dataNodeHosts);
    this.dfsCluster.waitClusterUp();

    // Start up a zk cluster.
    if (this.zkCluster == null) {
      startMiniZKCluster(this.clusterTestBuildDir);
    }
    // TODO: to be fixed
    /*
      if (this.catalogCluster == null) {
        startCatalogCluster();
      }
      */

    return startMiniTajoCluster(this.clusterTestBuildDir, numSlaves, false);
  }

  public MiniTajoCluster startMiniClusterInLocal(final int numSlaves) throws Exception {
    // the conf is set to the distributed mode.
    this.conf.setBoolVar(ConfVars.CLUSTER_DISTRIBUTED, true);

    // If we already put up a cluster, fail.
    String testBuildPath = conf.get(TEST_DIRECTORY_KEY, null);
    isRunningCluster(testBuildPath);
    if (testBuildPath != null) {
      LOG.info("Using passed path: " + testBuildPath);
    }

    // Make a new random dir to home everything in.  Set it as system property.
    // minidfs reads home from system property.
    this.clusterTestBuildDir = testBuildPath == null?
        setupClusterTestBuildDir() : new File(testBuildPath);

    System.setProperty(TEST_DIRECTORY_KEY,
        this.clusterTestBuildDir.getAbsolutePath());

    // Start up a zk cluster.
    if (this.zkCluster == null) {
      startMiniZKCluster(this.clusterTestBuildDir);
    }

    return startMiniTajoCluster(this.clusterTestBuildDir, numSlaves, true);
  }



  public void shutdownMiniCluster() throws IOException {
    LOG.info("Shutting down minicluster");
    shutdownMiniTajoCluster();

    if(!this.passedZkCluster) shutdownMiniZKCluster();
    if(this.dfsCluster != null) {
      this.dfsCluster.shutdown();
    }

    if(this.clusterTestBuildDir != null && this.clusterTestBuildDir.exists()) {
      LocalFileSystem localFS = LocalFileSystem.getLocal(conf);
      localFS.delete(
          new Path(clusterTestBuildDir.toString()), true);
      this.clusterTestBuildDir = null;
    }
    if(this.catalogCluster != null) {
      shutdownCatalogCluster();
    }

    LOG.info("Minicluster is down");
  }

  public static ResultSet runInLocal(String[] tableNames,
                                     Schema[] schemas,
                                     Options option,
                                     String[][] tables,
                                     String query) throws Exception {
    TajoTestingUtility util = new TajoTestingUtility();
    util.startMiniClusterInLocal(1);
    TajoConf conf = util.getConfiguration();
    TajoClient client = new TajoClient(conf);

    File tmpDir = util.setupClusterTestBuildDir();
    for (int i = 0; i < tableNames.length; i++) {
      File tableDir = new File(tmpDir,tableNames[i]);
      tableDir.mkdirs();
      File dataDir = new File(tableDir, "data");
      dataDir.mkdirs();
      File tableFile = new File(dataDir, tableNames[i]);
      writeLines(tableFile, tables[i]);
      TableMeta meta = TCatUtil.newTableMeta(schemas[i], CatalogProtos.StoreType.CSV, option);
      client.createTable(tableNames[i], new Path(tableDir.getAbsolutePath()), meta);
    }
    Thread.sleep(1000);
    ResultSet res = client.executeQuery(query);
    util.shutdownMiniCluster();
    return res;
  }

  public static ResultSet run(String[] names,
                              String[] tablepaths,
                              Schema[] schemas,
                              Options option,
                              String query) throws Exception {
    TajoTestingUtility util = new TajoTestingUtility();
    util.startMiniCluster(1);
    TajoConf conf = util.getConfiguration();
    TajoClient client = new TajoClient(conf);

    FileSystem fs = util.getDefaultFileSystem();
    Path rootDir = util.getMiniTajoCluster().getMaster().
        getStorageManager().getDataRoot();
    fs.mkdirs(rootDir);
    for (int i = 0; i < tablepaths.length; i++) {
      Path localPath = new Path(tablepaths[i]);
      Path tablePath = new Path(rootDir, names[i]);
      fs.mkdirs(tablePath);
      Path dataPath = new Path(tablePath, "data");
      fs.mkdirs(dataPath);
      Path dfsPath = new Path(dataPath, localPath.getName());
      fs.copyFromLocalFile(localPath, dfsPath);
      TableMeta meta = TCatUtil.newTableMeta(schemas[i],
          CatalogProtos.StoreType.CSV, option);
      client.createTable(names[i], tablePath, meta);
    }
    Thread.sleep(1000);
    ResultSet res = client.executeQuery(query);
    util.shutdownMiniCluster();
    return res;
  }

  public static ResultSet run(String[] names,
                              Schema[] schemas,
                              Options option,
                              String[][] tables,
                              String query) throws Exception {
    TajoTestingUtility util = new TajoTestingUtility();
    util.startMiniCluster(1);
    TajoConf conf = util.getConfiguration();
    TajoClient client = new TajoClient(conf);

    FileSystem fs = util.getDefaultFileSystem();
    Path rootDir = util.getMiniTajoCluster().getMaster().
        getStorageManager().getDataRoot();
    fs.mkdirs(rootDir);
    for (int i = 0; i < names.length; i++) {
      Path tablePath = new Path(rootDir, names[i]);
      fs.mkdirs(tablePath);
      Path dataPath = new Path(tablePath, "data");
      fs.mkdirs(dataPath);
      Path dfsPath = new Path(dataPath, names[i] + ".tbl");
      FSDataOutputStream out = fs.create(dfsPath);
      for (int j = 0; j < tables[i].length; j++) {
        out.write((tables[i][j]+"\n").getBytes());
      }
      out.close();
      TableMeta meta = TCatUtil.newTableMeta(schemas[i],
          CatalogProtos.StoreType.CSV, option);
      client.createTable(names[i], tablePath, meta);
    }
    Thread.sleep(1000);
    ResultSet res = client.executeQuery(query);
    util.shutdownMiniCluster();
    return res;
  }

    /**
    * Write lines to a file.
    *
    * @param file File to write lines to
    * @param lines Strings written to the file
    * @throws IOException
    */
  private static void writeLines(File file, String... lines)
      throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      for (String line : lines) {
        writer.write(line);
        writer.write('\n');
      }
    } finally {
      Closeables.closeQuietly(writer);
    }
  }


	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		TajoTestingUtility cluster = new TajoTestingUtility();
    File f = cluster.setupClusterTestBuildDir();
    System.out.println("first setupClusterTestBuildDir: " + f);
    f = cluster.setupClusterTestBuildDir();
    System.out.println("second setupClusterTestBuildDir: " + f);
    f = cluster.getTestDir();
    System.out.println("getTestDir() after second: " + f);
    f = cluster.getTestDir("abc");
    System.out.println("getTestDir(\"abc\") after second: " + f);

    cluster.initTestDir();
    f = cluster.getTestDir();
    System.out.println("getTestDir() after initTestDir: " + f);
    f = cluster.getTestDir("abc");
    System.out.println("getTestDir(\"abc\") after initTestDir: " + f);
    f = cluster.setupClusterTestBuildDir();
    System.out.println("setupClusterTestBuildDir() after initTestDir: " + f);

    TajoTestingUtility cluster2 = new TajoTestingUtility();
    File f2 = cluster2.setupClusterTestBuildDir();
    System.out.println("first setupClusterTestBuildDir of cluster2: " + f2);
    /*
    String [] names = {"table1"};
    String [][] tables = new String[1][];
    tables[0] = new String[] {"a,b,c", "b,c,d"};

    Schema [] schemas = new Schema[1];
    schemas[0] = new Schema()
          .addColumn("f1", CatalogProtos.DataType.STRING)
          .addColumn("f2", CatalogProtos.DataType.STRING)
          .addColumn("f3", CatalogProtos.DataType.STRING);

    ResultSet res = runInLocal(names, schemas, tables, "select f1 from table1");
    res.next();
    System.out.println(res.getString(0));
    res.next();
    System.out.println(res.getString(0));
    System.exit(0);
    */
	}
}
