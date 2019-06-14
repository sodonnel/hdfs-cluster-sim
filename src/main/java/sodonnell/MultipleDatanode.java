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
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.log4j.Logger;

public class MultipleDatanode {
  
  static { DefaultMetricsSystem.setMiniClusterMode(true); }

  private final static Logger LOG = Logger.getLogger(MultipleDatanode.class);
  
  private final static String STORAGE_ROOT_KEY = "multipledatanode.storage.root";
  private final static String STORAGE_ROOT_DEFAULT = "/tmp/hadoop-multipledatanode/storage";
  
  private final static String SIMULATED_STORAGE_KEY = "multipledatanode.storage.simulated";
  private final static boolean SIMULATED_STORAGE_DEFAULT = true;
  
  private final static String BLOCK_POOL_ID_KEY = "multipledatanode.bpid";
  private final static String NAMENODE_STORAGE_DIR_KEY = "dfs.namenode.name.dir";
  
  private final static String NUMBER_OF_VOLUMES_KEY = "multipledatanode.storage.count";
  private final static int NUMBER_OF_VOLUMES_DEFAULT = 2;
  
  private final static String DN_STORAGE_KEY = "dfs.datanode.data.dir";
  
  public final static String DN_XFER_ADDRESS_KEY = "dfs.datanode.address";
  public final static String DN_XFER_ADDRESS_DEFAULT = "0.0.0.0:0";
  
  public final static String DN_IPC_ADDRESS_KEY = "dfs.datanode.ipc.address";
  public final static String DN_IPC_ADDRESS_DEFAULT = "0.0.0.0:0";
  
  public final static String DN_HTTP_ADDRESS_KEY = "dfs.datanode.http.address";
  public final static String DN_HTTP_ADDRESS_DEFAULT = "0.0.0.0:0";
  
  private static final String USAGE =
      "Usage: sodonnell.MultipleDatanode numberOfNodes\n" +
          "   numberOfNodes should be an integer specifing how many DNs to start in this JVM.\n";

  static void printUsageExit(String err) {
    System.out.println(err);
    System.out.println(USAGE);
    System.exit(1);
  }
  
  public static void main(String[] args) {
    if (args.length < 1) {
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
  private int volumesPerDn;
  
  public MultipleDatanode() {
    runningDataNodes = new ConcurrentHashMap<Integer, MultipleDatanodeInstance>();
    Configuration conf = new Configuration();
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    dataDirectoryRoot = conf.get(STORAGE_ROOT_KEY, STORAGE_ROOT_DEFAULT);
    volumesPerDn = conf.getInt(NUMBER_OF_VOLUMES_KEY, NUMBER_OF_VOLUMES_DEFAULT);
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
    LOG.info("Entered startDatanodes");
    MultipleDatanodeInstanceList results = new MultipleDatanodeInstanceList();
    List<Thread> startingThreads = new ArrayList<Thread>();
    for (int i=getMaxDnId()+1; i<=numberToStart; i++) {
      final int dn = i;
      Thread t = new Thread() {
        public void run() {
          try {
            results.addInstance(startDataNode(dn));
          } catch (Exception e) {
            results.addException(e);
          }
        }
      };
      t.start();
      startingThreads.add(t);
    }
    for (Thread t : startingThreads) {
      try {
        t.join();
      } catch (InterruptedException ie) {
        results.addException(ie);
      }
    }
    return results;
  }

  public MultipleDatanodeInstance startDataNode(int dnId)
      throws MultipleDatanodeException, MultipleDatanodeRunningException {
    MultipleDatanodeInstance dni = getOrCreateDni(dnId);
    if (!dni.isStopped()) {
      throw new MultipleDatanodeRunningException("DataNode "+dnId+" is already running");
    }
    try {
      dni.start();
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
      dni.stop();
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

  private synchronized MultipleDatanodeInstance getOrCreateDni(int dnId) {
    MultipleDatanodeInstance dni = runningDataNodes.get(dnId);
    if (dni != null) {
      return dni;
    }
    Configuration conf = new Configuration(false);
    conf.addResource("hdfs-site.xml");

    conf.set(DN_STORAGE_KEY, generateStorageDirs(dataDirectoryRoot, dnId, volumesPerDn));
    conf.set(DN_XFER_ADDRESS_KEY,
        conf.get(DN_XFER_ADDRESS_KEY, DN_XFER_ADDRESS_DEFAULT));
    conf.set(DN_IPC_ADDRESS_KEY,
        conf.get(DN_IPC_ADDRESS_KEY, DN_IPC_ADDRESS_DEFAULT));
    conf.set(DN_HTTP_ADDRESS_KEY,
        conf.get(DN_HTTP_ADDRESS_KEY, DN_HTTP_ADDRESS_DEFAULT));

    if (isSimulated) {
      SimulatedFSDataset.setFactory(conf);
    }
    dni = new MultipleDatanodeInstance(dnId, conf, blockPoolId, dataDirectoryRoot+"/"+dnId);
    runningDataNodes.put(dnId, dni);
    return dni;
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