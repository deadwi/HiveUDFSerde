package example.hive.serde.ux;

import java.io.IOException;

import example.hive.serde.FsLineReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class UXLogGenericRecordReader implements RecordReader<NullWritable, UXLogGenericRecordWritable>, JobConfigurable
{
	private static final Log LOG = LogFactory.getLog(UXLogGenericRecordReader.class);
	private JobConf jobConf;
	private FsLineReader lineReader;
	private UXLogReader uxLogReader;
	private long start;
	private long stop;

	public UXLogGenericRecordReader(JobConf job, FileSplit split, Reporter reporter) throws IOException
	{
		this.jobConf = job;
		
		Path path = split.getPath();
		lineReader = new FsLineReader(path.getFileSystem(job).open(path));
		lineReader.seekByNewline(split.getStart());
		start = split.getStart();
		stop = split.getStart() + split.getLength();

		uxLogReader = new UXLogReader(lineReader, stop);		
	}

	@Override
	public boolean next(NullWritable nullWritable, UXLogGenericRecordWritable record) throws IOException
	{
		// 데이터가 없는데 이미 범위를 넘긴 경우
		if(uxLogReader.IsEndItem()==true && getPos()>=stop)
			return false;
		
		Object object = uxLogReader.next();
		// 더 이상 데이터가 없는 경우
		if(object==null)
			return false;
		
		record.setRecord(object);
		return true;
	}

	@Override
	public NullWritable createKey()
	{
		return NullWritable.get();
	}

	@Override
	public UXLogGenericRecordWritable createValue()
	{
		return new UXLogGenericRecordWritable();
	}

	@Override
	public long getPos() throws IOException
	{
		return lineReader.getPos();
	}

	@Override
	public void close() throws IOException
	{
		lineReader.close();
	}

	@Override
	public float getProgress() throws IOException
	{
	    return stop == start ? 0.0f : Math.min(1.0f, (getPos() - start) / (float)(stop - start));
	}

	@Override
	public void configure(JobConf jobConf)
	{
		this.jobConf = jobConf;
	}
}
