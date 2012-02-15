/**
 * 
 */
package nta.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.TableProto;
import nta.engine.NConstants;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.storage.exception.AlreadyExistsStorageException;
import nta.util.FileUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 테이블 Scanner와 Appender를 열고 관리한다.
 * 
 * @author Hyunsik Choi
 *
 */
public class StorageManager {
	private final Log LOG = LogFactory.getLog(StorageManager.class);

	private final Configuration conf;
	private final FileSystem fs;
	private final Path dataRoot;

	public StorageManager(Configuration conf) throws IOException {
		this.conf = conf;
		this.fs = FileSystem.get(conf);
		this.dataRoot = new Path(conf.get(NConstants.ENGINE_DATA_DIR));
		if(!fs.exists(dataRoot)) {
		  fs.mkdirs(dataRoot);
		}
		LOG.info("Storage Manager initialized");
	}
	
	public static StorageManager get(Configuration conf) throws IOException {
	  return new StorageManager(conf);
	}
	
	public static StorageManager get(Configuration conf, String dataRoot) 
	    throws IOException {
	  conf.set(NConstants.ENGINE_DATA_DIR, dataRoot);
    return new StorageManager(conf);
	}
	
	public static StorageManager get(Configuration conf, Path dataRoot) 
      throws IOException {
    conf.set(NConstants.ENGINE_DATA_DIR, dataRoot.toString());
    return new StorageManager(conf);
  }
	
	public FileSystem getFileSystem() {
	  return this.fs;
	}
	
	public Path getDataRoot() {
	  return this.dataRoot;
	}
	
	public Path initTableBase(TableMeta meta, String tableName) 
	    throws IOException {
	  return initTableBase(new Path(dataRoot,tableName), meta);
	}
	
	public Path initTableBase(Path tablePath, TableMeta meta) 
	    throws IOException {
    if (fs.exists(tablePath)) {
      throw new AlreadyExistsStorageException(tablePath);
    }

    fs.mkdirs(tablePath);
    Path dataDir = new Path(tablePath,"data");
    fs.mkdirs(dataDir);
    if (meta != null)
      writeTableMeta(tablePath, meta);
    
    LOG.info("Initialized table root (" + tablePath + ")");
    return dataDir;
	}
	
	public void delete(String tableName) throws IOException {
	  fs.delete(new Path(dataRoot, tableName), true);
	}
	
  public void delete(Path tablePath) throws IOException {
    fs.delete(tablePath, true);
  }
  
  public Path getTablePath(String tableName) throws IOException {
    return new Path(dataRoot, tableName);
  }
	
	public Scanner getTableScanner(String tableName) throws IOException {
	  return getTableScanner(new Path(dataRoot,tableName));
	}
	
	public Scanner getTableScanner(Path tablePath) throws IOException {
	  TableMeta info = getTableMeta(tablePath);
    Fragment [] tablets = split(tablePath);
    return getScanner(info, tablets);
	}
	
	public Scanner getScanner(String tableName, String fileName) 
	    throws IOException {
	  TableMeta meta = getTableMeta(getTablePath(tableName));
	  Path filePath = StorageUtil.concatPath(dataRoot, tableName, 
	      "data", fileName);	  
	  FileStatus status = fs.getFileStatus(filePath);
	  Fragment tablet = new Fragment(tableName+"_1", status.getPath(), 
	      meta, 0l , status.getLen());
	  
	  return getScanner(meta, new Fragment[] {tablet});
	}
	
	public Scanner getScanner(TableMeta meta, Fragment [] tablets) throws IOException {
    Scanner scanner = null;
    
    switch(meta.getStoreType()) {
    case RAW: {
      scanner = new RawFile2(conf).
          openScanner(meta.getSchema(), tablets);     
      break;
    }
    case CSV: {
      scanner = new CSVFile2(conf).openScanner(meta.getSchema(), tablets);
      break;
    }
    }
    
    return scanner;
  }
	
	/**
	 * 파일을 outputPath에 출력한다. writeMeta가 true라면 TableMeta를 테이블 디렉토리에 저장한다.
	 * 
	 * @param conf
	 * @param meta 테이블 정보
	 * @param 출력할 파일 이름 
	 * @return
	 * @throws IOException
	 */
	public Appender getAppender(TableMeta meta, Path filename) 
	    throws IOException {
	  Appender appender = null;
    switch(meta.getStoreType()) {
    case RAW: {
      appender = new RawFile2(conf).getAppender(meta,
          filename);
      break;
    }
    case CSV: {
      appender = new CSVFile2(conf).getAppender(meta, filename);
      break;
    }
    }
    
    return appender; 
	}
	
	public Appender getAppender(TableMeta meta, String tableName, String filename) 
	    throws IOException {
	  Path filePath = StorageUtil.concatPath(dataRoot, tableName, "data", filename);
	  return getAppender(meta, filePath);
	}
	
	public Appender getTableAppender(TableMeta meta, String tableName) 
	    throws IOException {
	  return getTableAppender(meta, new Path(dataRoot, tableName));
	}
	
	public Appender getTableAppender(TableMeta meta, Path tablePath) throws 
  IOException {
    Appender appender = null;

    Path tableData = new Path(tablePath, "data");
    if (!fs.exists(tablePath)) {
      fs.mkdirs(tableData);
    } else {
      throw new AlreadyExistsStorageException(tablePath);
    }

    Path outputFileName = new Path(tableData, ""+System.currentTimeMillis());   

    appender = getAppender(meta, outputFileName);

    StorageUtil.writeTableMeta(conf, tablePath, meta);

    return appender;
  }
	
	public TableMeta getTableMeta(String tableName) throws IOException {
	  Path tableRoot = getTablePath(tableName);
	  return getTableMeta(tableRoot);
	}
	
	public TableMeta getTableMeta(Path tablePath) throws IOException {
    TableMeta meta = null;
    
    Path tableMetaPath = new Path(tablePath, ".meta");
    if(!fs.exists(tableMetaPath)) {
      throw new FileNotFoundException(".meta file not found in "+tablePath.toString());
    }
    
    FSDataInputStream tableMetaIn = fs.open(tableMetaPath);

    TableProto tableProto = (TableProto) FileUtil.loadProto(tableMetaIn, 
      TableProto.getDefaultInstance());
    meta = new TableMetaImpl(tableProto);

    return meta;
  }
	
	public FileStatus [] listTableFiles(String tableName) throws IOException {
	  Path dataPath = new Path(dataRoot,tableName);
	  return fs.listStatus(new Path(dataPath, "data"));
	}
	
	public Fragment[] split(String tableName) throws IOException {
	  Path tablePath = new Path(dataRoot, tableName);
	  return split(tablePath);
	}
	
	public Fragment[] split(Path tablePath)
      throws IOException {
    TableMeta meta = getTableMeta(tablePath);
	  
	  long defaultBlockSize = fs.getDefaultBlockSize();

    List<Fragment> listTablets = new ArrayList<Fragment>();
    Fragment tablet = null;

    FileStatus[] fileLists = fs.listStatus(new Path(tablePath, "data"));
    for (FileStatus file : fileLists) {
      long remainFileSize = file.getLen();
      long start = 0;
      if (remainFileSize > defaultBlockSize) {
        while (remainFileSize > defaultBlockSize) {
          tablet = new Fragment(tablePath.getName(), file.getPath(), meta, start,
              defaultBlockSize);
          listTablets.add(tablet);
          start += defaultBlockSize;
          remainFileSize -= defaultBlockSize;
        }
        listTablets.add(new Fragment(tablePath.getName(), file.getPath(), meta, start,
            remainFileSize));
      } else {
        listTablets.add(new Fragment(tablePath.getName(), file.getPath(), meta, 0,
            remainFileSize));
      }
    }

    Fragment[] tablets = new Fragment[listTablets.size()];
    listTablets.toArray(tablets);

    return tablets;
  }
	
	public Fragment getFragment(String fragmentId, TableMeta meta, Path path) 
	    throws IOException {
	  FileStatus status = fs.getFileStatus(path);	  
	  return new Fragment(fragmentId, path, meta, 0, status.getLen());
	}
	
	public FileStatus [] getTableDataFiles(Path tableRoot) throws IOException {
	  Path dataPath = new Path(tableRoot, "data");
	  return fs.listStatus(dataPath);
	}
	
	private void writeTableMeta(Path tableRoot, TableMeta meta) 
	    throws IOException {	  
    FSDataOutputStream out = fs.create(new Path(tableRoot, ".meta"));
    FileUtil.writeProto(out, meta.getProto());
    out.flush();
    out.close();
	}
}
