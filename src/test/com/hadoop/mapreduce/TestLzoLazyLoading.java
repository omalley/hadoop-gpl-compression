/*
 * This file is part of Hadoop-Gpl-Compression.
 *
 * Hadoop-Gpl-Compression is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Hadoop-Gpl-Compression is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hadoop-Gpl-Compression.  If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.examples.WordCount.TokenizerMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;

import junit.framework.TestCase;

public class TestLzoLazyLoading extends TestCase {
  public static class MyMapper extends TokenizerMapper {
    @Override
    protected void cleanup(Context context) {
      boolean isLzoChecked = context.getConfiguration().getBoolean(
          "mapred.compression.lzo.test.codec-checked-after-map", false);
      assertEquals("IsLzoChecked (map)?", isLzoChecked, LzoCodec.isNativeLzoChecked());
    }
  }

  public static class MyCombiner extends IntSumReducer {
    @Override
    protected void cleanup(Context context) {
      boolean isLzoChecked = context.getConfiguration().getBoolean(
          "mapred.compression.lzo.test.codec-checked-after-map", false);
      assertEquals("IsLzoChecked (combine)?", isLzoChecked, LzoCodec.isNativeLzoChecked());
    }
  }

  public static class MyReducer extends IntSumReducer {
    @Override
    protected void cleanup(Context context) {
      boolean isLzoChecked = context.getConfiguration().getBoolean(
          "mapred.compression.lzo.test.codec-checked-after-reduce", false);
      assertEquals("IsLzoChecked (reduce)?", isLzoChecked, LzoCodec.isNativeLzoChecked());
    }
  }
  
  private static Path TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp"));
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  static {
    conf.set("io.compression.codecs", LzoCodec.class.getName());
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  public static Path writeFile(String name, String data) throws IOException {
    Path file = new Path(TEST_ROOT_DIR + "/" + name);
    localFs.delete(file, false);
    CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
    OutputStream f;
    Compressor compressor = null;
    if (codec == null) {
      f = localFs.create(file);
    } else {
      compressor = CodecPool.getCompressor(codec);
      f = codec.createOutputStream(localFs.create(file), compressor);
    }
    
    f.write(data.getBytes());
    f.close();
    if (compressor != null) {
      CodecPool.returnCompressor(compressor);
    }
    return file;
  }

  public static String readFile(String name) throws IOException {
    Path file = new Path(TEST_ROOT_DIR + "/" + name);
    CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
    InputStream f;
    Decompressor decompressor = null;
    if (codec == null) {
      f = localFs.open(file);
    } else {
      decompressor = CodecPool.getDecompressor(codec);
      f = codec.createInputStream(localFs.open(file), decompressor);
    }
    BufferedReader b = new BufferedReader(new InputStreamReader(f));
    StringBuilder result = new StringBuilder();
    String line = b.readLine();
    while (line != null) {
      result.append(line);
      result.append('\n');
      line = b.readLine();
    }
    b.close();
    if (decompressor != null) {
      CodecPool.returnDecompressor(decompressor);
    }
    return result.toString();
  }

  private static String makeFileName(String name, boolean compressed) {
    if (!compressed) {
      return name;
    }
    
    return name + new LzoCodec().getDefaultExtension();
  }
  
  public void testWithLocal() throws Exception {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "file:///", 3);
      Configuration conf = mr.createJobConf();
      conf.set("io.compression.codecs", LzoCodec.class.getName());
      runWordCount(conf, false, false);
      runWordCount(conf, false, true);
      runWordCount(conf, true, false);
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  private void runWordCount(Configuration cf, boolean compressIn, boolean compressOut) throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration thisConf = new Configuration(cf);
    if (compressIn) {
      thisConf.setBoolean("mapred.compression.lzo.test.codec-checked-after-map", true);
    }
    
    if (compressOut) {
      thisConf.setBoolean("mapred.compression.lzo.test.codec-checked-after-reduce", true);
    }
    Path pathIn = new Path(TEST_ROOT_DIR + "/in");
    Path pathOut = new Path(TEST_ROOT_DIR + "/out");
    localFs.delete(pathIn, true);
    localFs.delete(pathOut, true);
    writeFile(makeFileName("in/part1", compressIn), "this is a test\nof word count test\ntest\n");
    writeFile(makeFileName("in/part2", compressIn), "more test");
    Job job = new Job(thisConf, "word count");
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    if (compressOut) {
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
    }
    FileInputFormat.addInputPath(job, pathIn);
    FileOutputFormat.setOutputPath(job, pathOut);
    job.submit();
    assertEquals("IsLzoChecked (client)?", compressIn, LzoCodec.isNativeLzoChecked());
    assertTrue(job.waitForCompletion(false));
    String result = readFile(makeFileName("out/part-r-00000", compressOut));
    System.out.println(result);
    assertEquals(
        "a\t1\ncount\t1\nis\t1\nmore\t1\nof\t1\ntest\t4\nthis\t1\nword\t1\n",
        result);
  }

}
