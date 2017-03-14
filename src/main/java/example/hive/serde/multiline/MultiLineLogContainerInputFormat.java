package example.hive.serde.multiline;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MultiLineLogContainerInputFormat
        extends FileInputFormat<NullWritable, MultiLineLogGenericRecordWritable> implements JobConfigurable {
  private JobConf jobConf;

  @Override
  public RecordReader<NullWritable, MultiLineLogGenericRecordWritable>
    getRecordReader(InputSplit inputSplit, JobConf jc, Reporter reporter) throws IOException {
	  
    return new MultiLineLogGenericRecordReader(jc, (FileSplit) inputSplit, reporter);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename)
  {
	  return true;
  }
  
  @Override
  public void configure(JobConf jobConf) {
    this.jobConf = jobConf;
  }
}
