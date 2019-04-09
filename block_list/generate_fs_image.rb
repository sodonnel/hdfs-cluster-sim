require "#{__dir__}/lib/generate_fs_image"

hdfs_image = HdfsImageCLIFactory.create
hdfs_image.generate
