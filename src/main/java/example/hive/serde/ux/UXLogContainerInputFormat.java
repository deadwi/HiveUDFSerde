package example.hive.serde.ux;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UXLogContainerInputFormat extends FileInputFormat<NullWritable, UXLogGenericRecordWritable> implements JobConfigurable
{
	private JobConf jobConf;

	@Override
	protected FileStatus[] listStatus(JobConf job) throws IOException
	{
		List<FileStatus> result = new ArrayList<FileStatus>();
		for (FileStatus file : super.listStatus(job))
		{
			result.add(file);
		}
		return result.toArray(new FileStatus[0]);
	}

	@Override
	public RecordReader<NullWritable, UXLogGenericRecordWritable> getRecordReader(InputSplit inputSplit,
			JobConf jc, Reporter reporter) throws IOException
	{
		return new UXLogGenericRecordReader(jc, (FileSplit) inputSplit, reporter);
	}

	@Override
	public void configure(JobConf jobConf)
	{
		this.jobConf = jobConf;
	}
}
