package example.hive.serde.multiline;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import example.hive.serde.FsLineReader;
import example.hive.serde.Names;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class MultiLineLogGenericRecordReader implements RecordReader<NullWritable, MultiLineLogGenericRecordWritable>,
		JobConfigurable
{
	private static final Log LOG = LogFactory.getLog(MultiLineLogGenericRecordReader.class);
  private JobConf jobConf;
  private FsLineReader lineReader;
  private MultiLineLogReader logReader;
  private long startPos;
  private long stopPos;

	public MultiLineLogGenericRecordReader(JobConf job, FileSplit split, Reporter reporter) throws IOException
	{
		this.jobConf = job;
		
		Path path = split.getPath();
		lineReader = new FsLineReader(path.getFileSystem(job).open(path));
		lineReader.seek(split.getStart());

		logReader = new BattleLogReader(lineReader, getSchema(job, split));

		startPos = lineReader.getPos();
		stopPos = split.getStart() + split.getLength();
	}

  private boolean insideMRJob(JobConf job) {
		return job != null
		           && (HiveConf.getVar(job, HiveConf.ConfVars.PLAN) != null)
		           && (!HiveConf.getVar(job, HiveConf.ConfVars.PLAN).isEmpty());
	}
	
	private String getSchema(JobConf job, FileSplit split) throws IOException
	{
		// from TBLPROPERTIES
		if(insideMRJob(job))
		{
			MapWork mapWork = Utilities.getMapWork(job);
			for (Map.Entry<String,PartitionDesc> pathsAndParts: mapWork.getPathToPartitionInfo().entrySet())
			{
				String partitionPath = pathsAndParts.getKey();
				if(pathIsInPartition(split.getPath(), partitionPath))
				{
					Properties props = pathsAndParts.getValue().getProperties();
					if(props.containsKey(Names.SCHEMA_NAME))
						return props.getProperty(Names.SCHEMA_NAME);
					else
						return null; // fail
				}
			}
		}
		// from serder
		String s = job.get(Names.TABLE_NAME);
		if (s != null)
		{
			LOG.info("Found the result log schema in the job: " + s);
			return s;
		}
		return null;
	}
	
	private boolean pathIsInPartition(Path split, String partitionPath)
	{
		boolean schemeless = split.toUri().getScheme() == null;
		if (schemeless)
		{
			String schemelessPartitionPath = new Path(partitionPath).toUri().getPath();
			return split.toString().startsWith(schemelessPartitionPath);
		}
		else
		{
			return split.toString().startsWith(partitionPath);
		}
	}
	
	@Override
	public boolean next(NullWritable nullWritable, MultiLineLogGenericRecordWritable record) throws IOException
	{
		while(true)
		{
			// 데이터가 없는데 이미 범위를 넘긴 경우
			if(logReader.IsEndItemInTable()==true && getPos()>=stopPos)
				return false;

			Map<String, String> object = logReader.nextInTable();
			// 더 이상 데이터가 없는 경우
			if(object==null)
				return false;
			// 데이터가 빈 경우
			if(object.isEmpty())
				continue;

			record.setRecord(object);
			return true;			
		}
	}

	@Override
	public NullWritable createKey()
	{
		return NullWritable.get();
	}

	@Override
	public MultiLineLogGenericRecordWritable createValue()
	{
		return new MultiLineLogGenericRecordWritable();
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
	    return stopPos == startPos ? 0.0f : Math.min(1.0f, (getPos() - startPos) / (float)(stopPos - startPos));
	}

	@Override
	public void configure(JobConf jobConf)
	{
		this.jobConf = jobConf;
	}
}
