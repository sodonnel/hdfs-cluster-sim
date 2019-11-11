class HdfsImage
  attr_accessor :level_one_count, :level_two_count, :files_per_directory, :replication_factor, :layout_version, :namespace_id, :block_count, :block_size_mb, :block_file_writer, :output_path, :layout_version

  ROOT_INODE = 16385
  ONE_MB     = 1024 * 1024
  INITIAL_BLOCK_ID = 1073741825
  INITIAL_GENSTAMP = 1001

  IMAGE_FILENAME = "fsimage.xml"

  def initialize
    @level_one_count = 256
    @level_two_count = 256
    @files_per_directory = 1000
    @replication_factor = 3
    @layout_version = "-65"
    @namespace_id = "2140017751"
    @block_count = 1000
    @block_size_mb = 10
    # Expect an initialized object of class DnBlockFileWriter
    @block_file_writer = nil
    @output_path

    @leaf_inode_start = 0
    @leaf_inode_end   = 0
    @file_inode_start = 0
    @file_inode_end   = 0
  end

  def generate
    @fh = File.open(File.join(@output_path, IMAGE_FILENAME), "w")
    if @block_file_writer
      @block_file_writer.open(@output_path)
    end
    
    generate_header
    generate_inode_section
    generate_inode_directory_section
    generate_inode_reference_section
    generate_footer
    
    @fh.close
    if @block_file_writer
      @block_file_writer.close
    end
  end

  private

  def generate_inode_section
    # Generate a folder structure that is LEVEL_ONE * LEVEL_TWO directories.
    
    #                 root + generated + level_one_dirs           + level_two_dirs            + block/file count         
    last_inode_id = ROOT_INODE + 1 + @level_one_count + @level_one_count * @level_two_count + @block_count
    inode_count = @level_one_count + @level_one_count * @level_two_count + 2 + @block_count
    @fh.puts "<INodeSection><lastInodeId>#{last_inode_id}</lastInodeId><numInodes>#{inode_count}</numInodes>"
    
    inode_counter = ROOT_INODE
    # Generate the root inode
    output_inode_dir(inode_counter, "", "9223372036854775807")
    
    # Generate the folder called "/generated"
    inode_counter += 1
    output_inode_dir(inode_counter, "generated")
    
    # Now generate the base folder structure which has @level_one_count directories
    1.upto(@level_one_count) do |i|
      inode_counter += 1
      output_inode_dir(inode_counter, "level1_#{(i-1).to_s.rjust(4, "0")}")
    end
    
    # Add in the level two folders. Each level_1 folder has level_two_count sub-directories
    @leaf_inode_start = inode_counter + 1
    1.upto(@level_one_count) do |i|
      1.upto(@level_two_count) do |j|
        inode_counter += 1
        output_inode_dir(inode_counter, "level2_#{(j-1).to_s.rjust(4, "0")}")
      end
    end
    @leaf_inode_end = inode_counter
    
    # Now generate an inode per file
    block_id_counter = INITIAL_BLOCK_ID - 1
    gen_stamp_counter = INITIAL_GENSTAMP - 1
    @file_inode_start = inode_counter + 1
    
    1.upto(@block_count) do |i|
      inode_counter += 1
      block_id_counter += 1
      gen_stamp_counter += 1
      output_inode_file(inode_counter, "file_#{i.to_s.rjust(11, "0")}", block_id_counter, gen_stamp_counter, @replication_factor, ONE_MB*@block_size_mb)
      if @block_file_writer
        # output the block to the blocklists
        @block_file_writer.write_block(block_id_counter, gen_stamp_counter, ONE_MB*@block_size_mb, @replication_factor)
      end
    end
    @file_inode_end = inode_counter
    
    # end the section
    @fh.puts "</INodeSection>"
  end

  def generate_inode_directory_section
    @fh.puts "<INodeDirectorySection>"
    inode_counter = ROOT_INODE

    # First we have to link the top level folder called 'generated' to the root inode.
    # The root inode is always the initial_inode_id, and the generated folder is the next one.
    inode_counter += 1
    @fh.puts "<directory><parent>#{ROOT_INODE}</parent><child>#{inode_counter}</child></directory>"

    # Now we link all the level_1 folders to the generated folder, eg /generate/level_0, /generated/level_1, etc
    base_id = inode_counter
    @fh.puts "<directory><parent>#{inode_counter}</parent>"
    1.upto(@level_one_count) do |i|
      inode_counter += 1
      @fh.puts "<child>#{inode_counter}</child>"
    end
    @fh.puts "</directory>"

    # Now add each of the level two folders to the correct parent. In each case, we calculate
    # the inode id of the parent folder as we control the number of items created.
    1.upto(@level_one_count) do |i|
      parent_id = inode_counter - @level_one_count - @level_two_count * (i - 1) + i
      @fh.puts "<directory><parent>#{parent_id}</parent>"
      1.upto(@level_two_count) do |j|
        inode_counter += 1
        @fh.puts "<child>#{inode_counter}</child>"      
      end
      @fh.puts "</directory>"
    end

    # At this stage the directory structure has been created. Now we want to link the generated file names
    # into the leaf folders. This is relatively easy, as we know the start of the file inode IDs, and we
    # know the range of leaf folder IDs, so we simply fill the folders one at a time with X files.
    parent_id = @leaf_inode_start
    counter = 0
    @file_inode_start.upto(@file_inode_end) do |i|
      if counter == 0
        if parent_id > @file_inode_end
          raise "Exceeded the leaf directory count. Consider increasing files per directory"
        end
        @fh.puts "<directory><parent>#{parent_id}</parent>"
      end
      @fh.puts "<child>#{i}</child>"
      counter += 1
      
      if counter == @files_per_directory
        @fh.puts "</directory>"
        counter = 0
        parent_id += 1
      end
    end
    if counter != 0
      @fh.puts "</directory>"
    end
  
    @fh.puts "</INodeDirectorySection>"
  end
  
  def output_inode_dir(id, name, quota="-1")
    @fh.puts "<inode><id>#{id}</id><type>DIRECTORY</type><name>#{name}</name><mtime>1554197634733</mtime><permission>hdfs:supergroup:0755</permission><nsquota>#{quota}</nsquota><dsquota>-1</dsquota></inode>"
  end

  def output_inode_file(id, name, block_id, genstamp, rep_factor, size)
    @fh.puts "<inode><id>#{id}</id><type>FILE</type><name>#{name}</name><replication>#{rep_factor}</replication><mtime>1554285467270</mtime><atime>1554285466577</atime><preferredBlockSize>134217728</preferredBlockSize><permission>hdfs:supergroup:0644</permission><blocks><block><id>#{block_id}</id><genstamp>#{genstamp}</genstamp><numBytes>#{size}</numBytes></block>
</blocks><storagePolicyId>0</storagePolicyId></inode>"
  end

  def generate_header
    last_genstamp = INITIAL_GENSTAMP + @block_count
    last_block_id = INITIAL_BLOCK_ID + @block_count

  @fh.puts "<?xml version=\"1.0\"?>
<fsimage><version><layoutVersion>#{@layout_version}</layoutVersion><onDiskVersion>1</onDiskVersion><oivRevision>cfc9d666e5457312afdd9e215e9691667d736796</oivRevision></version>
<NameSection><namespaceId>#{@namespace_id}</namespaceId><genstampV1>1000</genstampV1><genstampV2>#{last_genstamp}</genstampV2><genstampV1Limit>0</genstampV1Limit><lastAllocatedBlockId>#{last_block_id}</lastAllocatedBlockId><txid>0</txid></NameSection>
<ErasureCodingSection>
<erasureCodingPolicy>
<policyId>1</policyId><policyName>RS-6-3-1024k</policyName><cellSize>1048576</cellSize><policyState>DISABLED</policyState><ecSchema>
<codecName>rs</codecName><dataUnits>6</dataUnits><parityUnits>3</parityUnits></ecSchema>
</erasureCodingPolicy>

<erasureCodingPolicy>
<policyId>2</policyId><policyName>RS-3-2-1024k</policyName><cellSize>1048576</cellSize><policyState>DISABLED</policyState><ecSchema>
<codecName>rs</codecName><dataUnits>3</dataUnits><parityUnits>2</parityUnits></ecSchema>
</erasureCodingPolicy>

<erasureCodingPolicy>
<policyId>3</policyId><policyName>RS-LEGACY-6-3-1024k</policyName><cellSize>1048576</cellSize><policyState>DISABLED</policyState><ecSchema>
<codecName>rs-legacy</codecName><dataUnits>6</dataUnits><parityUnits>3</parityUnits></ecSchema>
</erasureCodingPolicy>

<erasureCodingPolicy>
<policyId>4</policyId><policyName>XOR-2-1-1024k</policyName><cellSize>1048576</cellSize><policyState>DISABLED</policyState><ecSchema>
<codecName>xor</codecName><dataUnits>2</dataUnits><parityUnits>1</parityUnits></ecSchema>
</erasureCodingPolicy>

<erasureCodingPolicy>
<policyId>5</policyId><policyName>RS-10-4-1024k</policyName><cellSize>1048576</cellSize><policyState>DISABLED</policyState><ecSchema>
<codecName>rs</codecName><dataUnits>10</dataUnits><parityUnits>4</parityUnits></ecSchema>
</erasureCodingPolicy>

</ErasureCodingSection>"
  end

  def generate_inode_reference_section
  @fh.puts "<INodeReferenceSection></INodeReferenceSection><SnapshotSection><snapshotCounter>0</snapshotCounter><numSnapshots>0</numSnapshots></SnapshotSection>"
end

  def generate_footer
  @fh.puts "<FileUnderConstructionSection></FileUnderConstructionSection>
<SnapshotDiffSection></SnapshotDiffSection>
<SecretManagerSection><currentId>0</currentId><tokenSequenceNumber>0</tokenSequenceNumber><numDelegationKeys>0</numDelegationKeys><numTokens>0</numTokens></SecretManagerSection><CacheManagerSection><nextDirectiveId>1</nextDirectiveId><numDirectives>0</numDirectives><numPools>0</numPools></CacheManagerSection>
</fsimage>"
  end
  
end


class DnBlockFileWriter

  attr_reader :dn_count

  def initialize(dn_count)
    @dn_count = dn_count
    @fh_list = Array.new
    @last_index = 0
  end

  def open(path)
    open_file_handles(path)
  end

  def close
    close_file_handles
  end
  
  def write_block(block_id, gen_stamp, size, replication_factor=3)
    if replication_factor > @dn_count
      raise "Not enough datanodes #{@dn_count} for replication factor #{replication_factor}"
    end

    str = "#{block_id},#{gen_stamp},#{size}"    
    ind = @last_index
    1.upto(replication_factor) do |i|
      @fh_list[ind].puts str
      ind = increment_index(ind)
    end
    @last_index = increment_index(@last_index)
  end
  
  private

  def increment_index(ind)
    (ind + 1) % @dn_count
  end

  def open_file_handles(path)
    1.upto(@dn_count) do |i|
      @fh_list.push File.open("#{path}/blockList_#{i}", "w")
    end
  end

  def close_file_handles
    @fh_list.each {|fh| fh.close}
  end
end

require 'optparse'

class HdfsImageCLIFactory

  def self.create
    options = {
      :output_path => '.',
      :block_count => 1000,
      :block_size_mb => 10,
      :replication_factor => 3,
      :datanode_count => 0,
      :files_per_directory => 1000,
      :layout_version => -65,
      :level_one_count => 256,
      :level_two_count => 256
    }

    OptionParser.new do |opt|
      opt.on('-b', '--block-count BLOCK_COUNT', 'Number of files to create with 1 block per file (1000)') { |o| options[:block_count] = o.to_i }
      opt.on('-o', '--output-path PATH', 'Folder where the fsimage and any blocklists will be generated. The image will be named fsimage.xml (current_folder)') { |o| options[:output_path] = o }
      opt.on('-s', '--block-size-mb SIZE', 'Size in MB of each block (10)') { |o| options[:block_size_mb] = o.to_f }
      opt.on('-r', '--replication-factor NUM', 'Replication factor of each file (3)') { |o| options[:replication_factor] = o.to_i }
      opt.on('-d', '--datanode-count NUM', 'Number of datanode block list files. Setting to zero disables generating block lists (0)') { |o| options[:datanode_count] = o.to_i }
      opt.on('-f', '--files-per-directory NUM', 'Number of files in each generated directory (1000)') { |o| options[:files_per_directory] = o.to_i }
      opt.on('-v', '--version NUM', 'The HDFS FSImage layout version, eg -64, -65 etc (-65)') { |o| options[:layout_version] = o.to_i }
      opt.on('--level-one-count NUM', 'Number of folders in the top level directory (256)') { |o| options[:level_one_count] = o.to_i }
      opt.on('--level-two-count NUM', 'Number of sub-folders in each top level directory (256)') { |o| options[:level_two_count] = o.to_i }
    end.parse!

    bfw = nil
    if options[:datanode_count] > 0
      bfw = DnBlockFileWriter.new(options[:datanode_count])
    end

    img = HdfsImage.new.tap do |img|
      img.block_count = options[:block_count]
      img.block_size_mb = options[:block_size_mb]
      img.replication_factor = options[:replication_factor]
      img.files_per_directory = options[:files_per_directory]
      img.level_one_count = options[:level_one_count]
      img.level_two_count = options[:level_two_count]
      img.output_path = options[:output_path]
      img.layout_version = options[:layout_version]
      img.block_file_writer = bfw
    end
  end

end
