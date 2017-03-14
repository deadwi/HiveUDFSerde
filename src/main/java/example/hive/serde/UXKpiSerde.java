package example.hive.serde;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import example.hive.serde.ux.UXClickEvent;
import example.hive.serde.ux.UXLogGenericRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class UXKpiSerde extends AbstractSerDe
{
	private static final String FIELD_MILLISECOND = "millisecond";
	private static final String FIELD_POS_X = "pos_x";
	private static final String FIELD_POS_Y = "pos_y";
	private static final String FIELD_TARGET = "target";

	private StructTypeInfo rowTypeInfo;
	private ObjectInspector rowOI;
	private List<String> colNames;
	private List<TypeInfo> colTypes;
	private List<Object> row;

	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException
	{
		String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
		colNames = Arrays.asList(colNamesStr.split(","));
		for(int i=0;i<colNames.size();i++)
			colNames.set(i, colNames.get(i).toLowerCase());

		String colTypesStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);

		rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
		rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);

		row = new ArrayList<Object>(colNames.size());
		for (int c = 0; c < colNames.size(); c++)
			row.add(null);
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException
	{
		if(!(blob instanceof UXLogGenericRecordWritable))
			throw new SerDeException("Expecting a UXLogGenericRecordWritable");
		UXLogGenericRecordWritable recordWritable = (UXLogGenericRecordWritable) blob;
		UXClickEvent item = (UXClickEvent)recordWritable.getRecord();
		
		for (int c = 0; c < row.size(); c++)
			row.set(c, null);

		for (int i = 0; i < colNames.size(); i++)
		{
			String fieldName = colNames.get(i);
			String v = null;
			if(fieldName.compareToIgnoreCase(FIELD_MILLISECOND)==0)
				v = ""+item.timestamp;
			else if(fieldName.compareToIgnoreCase(FIELD_POS_X)==0)
				v = ""+item.posX;
			else if(fieldName.compareToIgnoreCase(FIELD_POS_Y)==0)
				v = ""+item.posY;
			else if(fieldName.compareToIgnoreCase(FIELD_TARGET)==0)
				v = item.target;
			if(v!=null)
				row.set(i, getTypeObjectFromString(v, colTypes.get(i)));
		}
		return row;
	}

	private Object getTypeObjectFromString(String t, TypeInfo type) throws SerDeException
	{
		try
		{
			PrimitiveTypeInfo pti = (PrimitiveTypeInfo) type;
			switch (pti.getPrimitiveCategory())
			{
			case STRING:
				return t;
			case BYTE:
				return Byte.valueOf(t);
			case SHORT:
				return Short.valueOf(t);
			case INT:
				return Integer.valueOf(t);
			case LONG:
				return Long.valueOf(t);
			case FLOAT:
				return Float.valueOf(t);
			case DOUBLE:
				return Double.valueOf(t);
			case BOOLEAN:
				return Boolean.valueOf(t);
			case TIMESTAMP:
				return Timestamp.valueOf(t);
			default:
				throw new SerDeException("Unsupported type " + type);
			}
		}
		catch (RuntimeException e)
		{
		}
		return null;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException
	{
		return rowOI;
	}

	/**
	 * Unimplemented
	 */
	@Override
	public SerDeStats getSerDeStats()
	{
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass()
	{
		return Text.class;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector oi) throws SerDeException
	{
		throw new UnsupportedOperationException("UxKpiSerde doesn't support the serialize() method");
	}

}
