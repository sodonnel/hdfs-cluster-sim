# HDFS Cluster Simulator

Testing how HDFS behaves when large numbers of nodes are decommissioned, recommissioned, stopped or balanced is challenging, as it it is unlikely you have enough hardware to stage another full sized environment, and some problems only happen at a certain scale.

Inspired by [Dynamometer](https://github.com/linkedin/dynamometer), this tool allows a HDFS Image to be created with a configurable number of blocks and block size. Then a configurable number of datanodes with simulated storage can be created, where many datanodes run inside the same JVM and on the same host. This allows a simulated cluster to be created on a small amount of hardware, while allowing most cluster operations to be performed as normal.

## Generating Cluster Metadata

### Namenode Metadata

The HDFS namenode stores all the cluster metadata in a single file called the fsimage. HDFS has a tool called `oiv` - the offline image viewer. This allows an image to be converted from its binary format, to an XML representation, and it also allows an XML representation to be convered into the binary image the namenode can use to start up.

Therefore, it is possible to generate the XML containing an abritary number of files / blocks and block sizes, allow a large HDFS filesystem to be created quicky and easily.

### Datanode Block Data

In a real cluster, a datanode stores metadata about each block it hosts and also the actual data bytes which have been written to HDFS. For many cluster maintenance operations, the actual block data is not important and just the presence of the block is all that is needed when decommissioning or balancing nodes in the cluster. To allow a large cluster to be created with minimal stored, the datanodes can be configured to use a simulated storage (SimulatedFSDataset). This stores the block metadata in memory in a similar way to normal datanodes, but the simulated storage does not store the block data on disk. Instead, it simply discards the data which is written to it, and on reads, returns a fixed byte sequence that repeats to the required length. For this to work effectively, [HDFS-12818](https://issues.apache.org/jira/browse/HDFS-12818) needs to be included in the HDFS build, as otherwise the SimulatedFSDataset stores all data on a single 'volume' which impacts how block reports are sent to the namenode.

Another feature of the SimulatedFSDataset is that it allows a list of blocks to be injected into the running datanode. When generating the fsimage for the namenode metadata, the list of block Ids for the cluster is known and therefore a list of blocks for a given number of datanodes can be generated at the same time.

### Generating the Image and Block Lists

The code to generate the image is written in Ruby, so it must be installed. From the root of this repo, you can run the utility:

```
ruby generate_fs_image.rb -h
Usage: generate_fs_image [options]
    -b, --block-count BLOCK_COUNT    Number of files to create with 1 block per file (1000)
    -o, --output-path PATH           Folder where the fsimage and any blocklists will be generated. The image will be named fsimage.xml (current_folder)
    -s, --block-size-mb SIZE         Size in MB of each block (10)
    -r, --replication-factor NUM     Replication factor of each file (3)
    -d, --datanode-count NUM         Number of datanode block list files. Setting to zero disables generating block lists (0)
    -f, --files-per-directory NUM    Number of files in each generated directory (1000)
        --level-one-count NUM        Number of folders in the top level directory (256)
        --level-two-count NUM        Number of sub-folders in each top level directory (256)
```

To generate a fsimage with the defaults (ie 1000 files of 10MB) simply run:

```
ruby generate_fs_image.rb
```

This will create a file called `fsimage.xml` in the current folder of about 14MB which has the following folder structure:

* A folder /generated at the root level
* Inside /generated are `--level-one-count` sub folders
* Inside each level 1 folder are `--level-two-count` folder.
* Starting from the first level 1 folder and the first level2 folder in it, files are added with 1000 files per directory

For example

```
/generated/level1_0/level2_0/file_1
/generated/level1_0/level2_0/file_2
...
/generated/level1_0/level2_1/file_1001
...
```

The bulk of the 14MB is for the 256x256 default folder structure.

You can then pass any of the options above to conntrol the block size, replication factor etc.

By default no datanode block lists are generated. To have them generated, simply pass `--datanode-count` to a value greater than zero and greater than or equal to the replication factor. Generally 3 is a minimum. By passing the datanode-count, a further set of files named like `blockList_n` will be created in the `--output-path` and they can later be used to bootstrap simulated datanodes.

The generated fsimage.xml can be created to a binary HDFS fsimage by running:

```
hdfs oiv -i fsimage.xml -o fsimage -p ReverseXML
```

This can then be used to boot a namenode.

### Notes

* Generating an image with 10M files takes about 36 seconds and 4GB of disk on my laptop.
* Also generating the blockList files for 20 DNs pushes the runtime up to 55 seconds and another 800MB of disk.
* We could write the output to a gzipped stream directly, but right now oiv will not a gzipped input stream. The above image, gzipped, goes from 4GB to 137MB so it may be worth looking into extending oiv.
* Having oiv convert the above xml into the binary image takes 1 minute 6 seconds about and creates an image of 741MB.


## Running a Simple Simulated Cluster Locally.

The idea behind this tool, is that you can start a namenode running, and then start several datanodes running inside a single JVM. These datanodes can either use simulated storage (useful for testing the balancer, decommission / recomission of nodes, etc) or write their blocks to local disk for experimenting with HDFS on your local system.

Therefore, the first step is to get the namenode running, either as a brand new filesystem, or using a generated image as described above.

After that, you can use the CommandShell class in this repo to start a simple command line shell which allows you to start and stop datanode instances (all within the same JVM) and have them connect to the running namenode.

### Building The Code

This is a maven project, so you can build the code as usual:

```
mvn package
```

The resulting jar will be:

```
target/DnSim-1.0-SNAPSHOT.jar
```

If you check the pom.xml, the project depends on `3.0.0-cdh6.1.0` at the current time. It should against Apache HDFS too, but this has not yet been tested.


### Running the Code

Assuming you have a namenode running locally, you need to ensure all the CDH jars and the above jar are on the CLASSPATH, and then run the Command shell as below:

```
export CLASSPATH=`hadoop classpath`:target/DnSim-1.0-SNAPSHOT.jar
java -Dlog4j.configuration=/Users/sodonnell/source/cluster_sim/conf/log4j.properties sodonnell.CommandShell
```

Note the path to the custom log4j.properties which prevent the Datanode logging going to the console by default.

Then, when in the command shell you can start and stop datanodes as follows:

```
# Start 5 datanodes, with IDs starting at 1 to 5.
> start 5

# Start another datanode, ID 6
> start #6

# Start 10 datanodes with IDs 1 to 10 - Note 1 - 5 will already be running and will give an error
> start 10

# Stop datanode ID 3
> stop #3

# Start datanode ID 3 again, reusing its old ports
> start #3

# Get Help
> help
```

### Relevant Parameters

TODO

## Running a Larger Cluster, injecting blocks etc

TODO
