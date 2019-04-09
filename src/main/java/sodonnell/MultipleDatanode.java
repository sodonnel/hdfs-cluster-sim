package sodonnell;


/*
 * dfs.datanode.simulateddatastorage.capacity
 * 
 * 
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

public class MultipleDatanode {
  
  static { DefaultMetricsSystem.setMiniClusterMode(true); }
  
  private final static String STORAGE_ROOT_KEY = "multipledatanode.storage.root";
  private final static String STORAGE_ROOT_DEFAULT = "/tmp/hadoop-multipledatanode/storage";
  
  private final static String SIMULATED_STORAGE_KEY = "multipledatanode.storage.simulated";
  private final static boolean SIMULATED_STORAGE_DEFAULT = true;
  
  private final static String BLOCK_POOL_ID_KEY = "multipledatanode.bpid";
  private final static String NAMENODE_STORAGE_DIR_KEY = "dfs.namenode.name.dir";
  
  private final static String NUMBER_OF_VOLUMES_KEY = "multipledatanode.storage.count";
  private final static int NUMBER_OF_VOLUMES_DEFAULT = 2;
  
  private final static String DN_STORAGE_KEY = "dfs.datanode.data.dir";
  
  private final static String DN_XFER_ADDRESS_KEY = "dfs.datanode.address";  
  private final static String DN_XFER_ADDRESS_DEFAULT = "0.0.0.0:0";
  
  private final static String DN_IPC_ADDRESS_KEY = "dfs.datanode.ipc.address";  
  private final static String DN_IPC_ADDRESS_DEFAULT = "0.0.0.0:0";
  
  private final static String DN_HTTP_ADDRESS_KEY = "dfs.datanode.http.address";  
  private final static String DN_HTTP_ADDRESS_DEFAULT = "0.0.0.0:0";
  
  private static final String USAGE =
      "Usage: sodonnell.MultipleDatanode numberOfNodes\n" +
          "   numberOfNodes should be an integer specifing how many DNs to start in this JVM.\n" +
          "   storageRoot path inside which the dataNode data directories will be created.\n";

  static void printUsageExit(String err) {
    System.out.println(err);
    System.out.println(USAGE);
    System.exit(1);
  }
  
  public static void main(String[] args) {
    if (args.length < 2) {
      printUsageExit("Insufficent argumenets");
      System.out.println("");
    }
    
    MultipleDatanode MDNs = new MultipleDatanode();    
    MDNs.startDatanodes(Integer.parseInt(args[0]));
  }
  
  private Map<Integer, MultipleDatanodeInstance> runningDataNodes;
  private String dataDirectoryRoot;
  private String blockPoolId;
  private Boolean isSimulated = SIMULATED_STORAGE_DEFAULT;
  
  public MultipleDatanode() {
    runningDataNodes = new HashMap<Integer, MultipleDatanodeInstance>();
    Configuration conf = new Configuration();
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    dataDirectoryRoot = conf.get(STORAGE_ROOT_KEY, STORAGE_ROOT_DEFAULT);
    setBlockPoolId(conf);
    System.out.println("BPID: "+blockPoolId);
    isSimulated =  conf.getBoolean(SIMULATED_STORAGE_KEY, SIMULATED_STORAGE_DEFAULT);
  }
  
  // I have not been able to find a way to extract the BPID from a registered
  // DN instance via any public API. Therefore there are two ways it can be obtained:
  // 1 - Read it from the namenode VERSION file within its storage directory
  // 2 - If the NN is remote, set it in hdfs-site.xml
  private void setBlockPoolId(Configuration conf) {
    // Check if the block pool has been manually set in the DN conf
    blockPoolId = conf.get(BLOCK_POOL_ID_KEY);
    if (blockPoolId == null) {
      // If not in the conf, to find it in the NN storage directory
      String nnDir = null;
      String nnConfDir = conf.get(NAMENODE_STORAGE_DIR_KEY);
      if (nnConfDir != null) {
        try {
          nnDir = new URI(nnConfDir).getPath();
        } catch (URISyntaxException e) {
          // TODO - log a warning no BPID found plus error
          e.printStackTrace();
          return;
        }
      }

      if (nnDir != null && new File(nnDir+"/current/VERSION").exists()) {  
        try (BufferedReader br = new BufferedReader(
            new FileReader(nnDir+"/current/VERSION"))) {
          String line = br.readLine();
          while (line != null) {
            if (line.startsWith("blockpoolID")){
              String[] parts = line.split("=");
              if (parts.length == 2) {
                blockPoolId = parts[1];
              }
              break;
            }
            line = br.readLine();
          }
       // } catch (FileNotFoundException e) {
       //   // TODO Auto-generated catch block
       //   e.printStackTrace();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } else {
        // TODO - log a warning, no BPID found
      }
    }
  }
  
  public MultipleDatanodeInstanceList startDatanodes(int numberToStart) { 
    MultipleDatanodeInstanceList results = new MultipleDatanodeInstanceList(); 
    for (int i=getMaxDnId()+1; i<=numberToStart; i++) {
      try {
        results.addInstance(startDataNode(i));
      } catch (Exception e) {
        results.addException(e);  
      }
    }
    return results;
  }
  
  public synchronized MultipleDatanodeInstance startDataNode(int dnId) 
      throws MultipleDatanodeException, MultipleDatanodeRunningException {
    MultipleDatanodeInstance dni = runningDataNodes.get(dnId);
    if (dni != null  && !dni.isStopped()) {
      throw new MultipleDatanodeRunningException("DataNode "+dnId+" is already running");
    }
    
    try {
      // Explicitly asking for a Conf without defaults, and then loading hdfs-site.xml
      // This allows us to use either the value passed in hdfs-site.xml, or the default
      // hardcoded here, especially for the DN address keys.
      Configuration conf = new Configuration(false);
      conf.addResource("hdfs-site.xml");
     
      int numVolumes = conf.getInt(NUMBER_OF_VOLUMES_KEY, NUMBER_OF_VOLUMES_DEFAULT);
      conf.set(DN_STORAGE_KEY, generateStorageDirs(dataDirectoryRoot, dnId, numVolumes));
      
      conf.set(DN_XFER_ADDRESS_KEY,
          conf.get(DN_XFER_ADDRESS_KEY, DN_XFER_ADDRESS_DEFAULT));
      conf.set(DN_IPC_ADDRESS_KEY,
          conf.get(DN_IPC_ADDRESS_KEY, DN_IPC_ADDRESS_DEFAULT));
      conf.set(DN_HTTP_ADDRESS_KEY,
          conf.get(DN_HTTP_ADDRESS_KEY, DN_HTTP_ADDRESS_DEFAULT));
            
      if (dni != null) {
        // Reuse the same ports as before. This prevent a DN restart leaving a 'dead node'
        // that the NN will send clients to for 10 minutes if a DN is restarted and comes
        // back up with different ports.        
        conf.set(DN_XFER_ADDRESS_KEY,
            conf.get(DN_XFER_ADDRESS_KEY).replaceAll("^([^:]+:)\\d+$" ,"$1"+dni.getXferPort()));
        conf.set(DN_IPC_ADDRESS_KEY,
            conf.get(DN_IPC_ADDRESS_KEY).replaceAll("^([^:]+:)\\d+$" ,"$1"+dni.getIpcPort()));
        conf.set(DN_HTTP_ADDRESS_KEY,
            conf.get(DN_HTTP_ADDRESS_KEY).replaceAll("^([^:]+:)\\d+$" ,"$1"+dni.getInfoPort()));
      }
      if (isSimulated) {
        SimulatedFSDataset.setFactory(conf);
      }
      
      DataNode dn = DataNode.instantiateDataNode(new String[0], conf, null);
      dn.runDatanodeDaemon();

      // Wait for the DN to register with the NN
      // TODO - implement a timeout
      while (DataNodeTestUtils.getFSDataset(dn) == null) {
        Thread.sleep(100);
      }

      if (isSimulated) {
        String blockListFile = dataDirectoryRoot + "/" + dnId + "/blockList";
        if (new File(blockListFile).exists()) {
          dni.injectBlocksInFile(blockListFile, blockPoolId);
        }
      }

      if (dni == null) {
        dni = new MultipleDatanodeInstance(dnId, dn);
        runningDataNodes.put(dnId, dni);
      } else {
        dni.setNewDnInstance(dn);
      }

      return dni;
    } catch (IOException e) {
      throw new MultipleDatanodeException("Failed to start datanode "+dnId, e);
    } catch (InterruptedException e) {
      throw new MultipleDatanodeException("Failed to start datanode "+dnId, e);
    }
  }

  public synchronized MultipleDatanodeInstance stopDataNode(int dnId) 
      throws MultipleDatanodeRunningException, MultipleDatanodeException {
    MultipleDatanodeInstance dni = runningDataNodes.get(dnId);
    if (dni == null || dni.isStopped()) {
      throw new MultipleDatanodeRunningException("DataNode "+dnId+" is not running");
    }

    try {
      dni.getDn().shutdownDatanode(false);
      // TODO - Should we wait for the DN to shutdown before returning?
      dni.setStopped();
      return dni;
    } catch (IOException e) {
      throw new MultipleDatanodeException("Failed to stop datanode " + dnId, e);
    }   
  }

  public MultipleDatanodeInstance stopDataNodeOnPort(int port) 
      throws MultipleDatanodeRunningException, MultipleDatanodeException {
    for (int key : runningDataNodes.keySet()) {
      MultipleDatanodeInstance dni = runningDataNodes.get(key);
      if (dni.getXferPort() == port || dni.getInfoPort() == port || dni.getIpcPort() == port ) {
        return stopDataNode(key);
      }
    }
    throw new MultipleDatanodeRunningException("No DataNodes is running on port "+ port);  
  }
  
  private String generateStorageDirs(String root, int dnId, int num) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (int i=0; i<num; i++) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(root+"/"+dnId+"/"+i);      
    }
    return sb.toString();
  }
  
  private int getMaxDnId() {
    // TODO - actually work out the max ID from the hash map
    return 0;
  }

}
