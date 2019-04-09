package sodonnell;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.tools.DFSAdmin;

public class MultipleDatanodeInstance {
  
  private int XferPort = 0;
  private int InfoPort = 0;
  private int IpcPort  = 0;
  private DataNode dn = null;
  private int dnId;
  private boolean stopped = true;
  
  public MultipleDatanodeInstance(int lDnId, DataNode ldn) {
    dnId = lDnId;
    loadNewDnInstance(ldn);
  }
  
  public void setStopped() {
    stopped = true;
    // TODO - if the DN is not truly stopped, could this leak objects?
    dn = null;
  }
  
  public boolean isStopped() {
    return stopped;
  }
  
  public void setNewDnInstance(DataNode ldn) {
    loadNewDnInstance(ldn);
  }
  
  public int getXferPort() {
    return XferPort;
  }
  
  public int getInfoPort() {
    return InfoPort;
  }
  
  public int getIpcPort() {
    return IpcPort;
  }
  
  public int getDnId() {
    return dnId;
  }
  
  public DataNode getDn() {
    return dn;
  }
  
  public void injectBlocks(String bpid, Iterable<Block> blocksToInject) throws IOException {
    final FsDatasetSpi<?> dataSet = DataNodeTestUtils.getFSDataset(dn);
    if (!(dataSet instanceof SimulatedFSDataset)) {
      throw new IOException("injectBlocks is valid only for SimilatedFSDataset");
    }
    SimulatedFSDataset sdataset = (SimulatedFSDataset) dataSet;
    sdataset.injectBlocks(bpid, blocksToInject);
    dn.scheduleAllBlockReport(0);
  }

  // The block list file is in the format blockID,blockGenStamp,blockSize
  public void injectBlocksInFile(String filePath, String bpid) throws IOException {
    ArrayList<Block> blockList = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(
        new FileReader(filePath))) {
      String line = br.readLine();
      while (line != null) {
        String[] parts = line.split(",");
        if (parts.length != 3) {
          // Bail out
        }
        blockList.add(new Block(Long.parseLong(parts[0]),
            Long.parseLong(parts[2]), Long.parseLong(parts[1])));
        line = br.readLine();
      }
    } catch (FileNotFoundException e) {

    } catch (IOException e) {

    }
    injectBlocks(bpid, blockList);
  }

  @Override
  public String toString() {
    return "Datanode "+dnId+" XferPort:" +XferPort +
        " InfoPort:" +InfoPort +
        " IpcPort:"  +IpcPort;
  }
  
  private void loadNewDnInstance(DataNode ldn) {
    dn = ldn;
    XferPort = dn.getXferPort();
    InfoPort = dn.getInfoPort();
    IpcPort  = dn.getIpcPort();
    stopped = false;  
  }

}