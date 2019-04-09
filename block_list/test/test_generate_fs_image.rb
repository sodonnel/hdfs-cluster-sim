require 'test/unit'
require "#{__dir__}/../lib/generate_fs_image.rb"

class TestBuildHtml < Test::Unit::TestCase

  def setup
  end

  def teardown
  end
  
  def test_all_cli_options_set
    ARGV.replace %w{--output-path test --block-count 999 --block-size-mb 998 --replication-factor 5 --datanode-count 3 --files-per-directory 997 --level-one-count 996 --level-two-count 995}
    h = HdfsImageCLIFactory.create

    assert_equal(h.output_path, "test")
    assert_equal(h.block_count, 999)
    assert_equal(h.block_size_mb, 998)
    assert_equal(h.replication_factor, 5)
    assert_equal(h.block_file_writer.dn_count, 3)
    assert_equal(h.files_per_directory, 997)
    assert_equal(h.level_one_count, 996)
    assert_equal(h.level_two_count, 995)    
  end
  
end
