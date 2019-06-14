package sodonnell;

import java.io.*;
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
import org.apache.log4j.Logger;

public class MultipleDatanodeInstance {

  private final static Logger LOG = Logger.getLogger(MultipleDatanodeInstance.class);
  
  private int XferPort = -1;
  private int InfoPort = -1;
  private int IpcPort  = -1;
  private DataNode dn = null;
  private int dnId;
  private boolean stopped = true;

  private Configuration conf;
  private String bpid;
  private String dataDirectoryRoot;
  
  public MultipleDatanodeInstance(int lDnId, Configuration config, String lbpid, String dataDirRoot) {
    dnId = lDnId;
    conf = config;
    bpid = lbpid;
    dataDirectoryRoot = dataDirRoot;
  }

  public synchronized void start() throws IOException, InterruptedException {
    LOG.info("DnID "+dnId+": About to start datanode process");
    if (XferPort != -1) {
      LOG.info("DnID "+dnId+": This datanode was previously started. Setting ports to existing values");
      conf.set(MultipleDatanode.DN_XFER_ADDRESS_KEY,
          conf.get(MultipleDatanode.DN_XFER_ADDRESS_KEY).replaceAll("^([^:]+:)\\d+$", "$1" + XferPort));
      conf.set(MultipleDatanode.DN_IPC_ADDRESS_KEY,
          conf.get(MultipleDatanode.DN_IPC_ADDRESS_KEY).replaceAll("^([^:]+:)\\d+$", "$1" + IpcPort));
      conf.set(MultipleDatanode.DN_HTTP_ADDRESS_KEY,
          conf.get(MultipleDatanode.DN_HTTP_ADDRESS_KEY).replaceAll("^([^:]+:)\\d+$", "$1" + InfoPort));
    }

    try {
      dn = DataNode.instantiateDataNode(new String[0], conf, null);
      dn.runDatanodeDaemon();
    } catch (IOException e) {
      LOG.error("DnID "+dnId+": Failed to start the datanode instance", e);
      throw e;
    }
    LOG.info("DnID "+dnId+": Started datanode instance. Attempting to register with Namenode");

    // Wait for the DN to register with the NN. Until it does the FSDataset returned will be null.
    // Note: this will retry forever.
    while (DataNodeTestUtils.getFSDataset(dn) == null) {
      Thread.sleep(100);
      LOG.info("DnID "+dnId+": Waiting for Namenode registration");
    }
    LOG.info("DnID "+dnId+": Registered with Namenode");
    loadNewDnInstance();

    if (isSimulated()) {
      LOG.info("DnID "+dnId+": Has simulated storage. Checking for blockList");
      String blockListFile = dataDirectoryRoot + "/blockList";
      if (new File(blockListFile).exists()) {
        LOG.info("DnID "+dnId+": Has a blockList at "+blockListFile);
        injectBlocksInFile(blockListFile, bpid);
      }
    }
  }

  public synchronized void stop() throws IOException {
    // TODO - can we wait here for the DN to shutdown?
    // TODO - is it safe to just set the dn instance to null, will it get GC'ed etc?
    dn.shutdownDatanode(false);
    stopped = true;
    dn = null;
  }
  
  public boolean isStopped() {
    return stopped;
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

  public boolean isSimulated() {
    if (dn == null) {
      return false;
    }
    FsDatasetSpi<?> dataSet = DataNodeTestUtils.getFSDataset(dn);
    return dataSet instanceof SimulatedFSDataset ? true : false;
  }
  
  public void injectBlocks(String bpid, Iterable<Block> blocksToInject) throws IOException {
    final FsDatasetSpi<?> dataSet = DataNodeTestUtils.getFSDataset(dn);
    if (!(dataSet instanceof SimulatedFSDataset)) {
      throw new IOException("injectBlocks is valid only for SimulatedFSDataset");
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
  
  private void loadNewDnInstance() {
    XferPort = dn.getXferPort();
    InfoPort = dn.getInfoPort();
    IpcPort  = dn.getIpcPort();
    stopped = false;  
  }

}