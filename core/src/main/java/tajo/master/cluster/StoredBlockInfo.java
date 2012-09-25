package tajo.master.cluster;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author jihoon
 */
public class StoredBlockInfo
    implements Comparable<StoredBlockInfo> {
  private final String host;
  private Map<Path, List<BlockLocation>> fileToBlocksMap; // filename, set of blocks
  private int numBlocks;
  private Entry<Path, List<BlockLocation>> current;
  private Iterator<Entry<Path, List<BlockLocation>>> outerIt = null;
  private Iterator<BlockLocation> innerIt = null;

  public StoredBlockInfo(String host) {
    this.host = host;
    fileToBlocksMap = Maps.newHashMap();
    numBlocks = 0;
  }

  public String getHost() {
    return this.host;
  }

  public void addBlock(Path filename, BlockLocation bl) {
    List<BlockLocation> set;
    if (fileToBlocksMap.containsKey(filename)) {
      set = fileToBlocksMap.get(filename);
    } else {
      set = new ArrayList<BlockLocation>();
    }
    set.add(bl);
    numBlocks++;
    fileToBlocksMap.put(filename, set);
  }

  public void addBlocks(Path filename, BlockLocation[] bls) {
    for (BlockLocation bl : bls) {
      this.addBlock(filename, bl);
    }
  }

  public boolean removeBlock(Path filename, BlockLocation bl) {
    if (fileToBlocksMap.containsKey(filename)) {
      List<BlockLocation> set = fileToBlocksMap.get(filename);
      if (set.contains(bl)) {
        numBlocks--;
        boolean result = set.remove(bl);
        if (set.size() == 0) {
          fileToBlocksMap.remove(filename);
        }
        return result;
      }
      return false;
    } else {
      return false;
    }
  }

  public int getBlockNum() {
    return this.numBlocks;
  }

  public Map<Path, List<BlockLocation>> getFileToBlocksMap() {
    return this.fileToBlocksMap;
  }

  public List<BlockLocation> getBlocks(Path filename) {
    return this.fileToBlocksMap.get(filename);
  }

  public boolean isExistBlock(Path filename, BlockLocation bl) {
    return fileToBlocksMap.get(filename).contains(bl);
  }

  @Override
  public int compareTo(StoredBlockInfo storedBlockInfo) {
    return this.numBlocks - storedBlockInfo.numBlocks;
  }

  public void resetIteration() {
    outerIt = fileToBlocksMap.entrySet().iterator();
    if (outerIt.hasNext()) {
      current = outerIt.next();
      innerIt = current.getValue().iterator();
    } else {
      current = null;
    }
  }

  public boolean hasNextBlock() {
    if (outerIt == null) {
      outerIt = fileToBlocksMap.entrySet().iterator();
    }
    if (innerIt != null) {
      return innerIt.hasNext();
    } else {
      if (outerIt.hasNext()) {
        if (innerIt == null) {
          current = outerIt.next();
          innerIt = current.getValue().iterator();
        }
        return innerIt.hasNext();
      } else {
        return false;
      }
    }
  }

  public BlockLocation nextBlock() {
    if (this.hasNextBlock()) {
      return innerIt.next();
    } else {
      return null;
    }
  }

  public Path getPathOfCurrentBlock() {
    return this.current.getKey();
  }

  @Override
  public String toString() {
    return "< " + this.host + " : " + this.numBlocks + " >";
  }
}
