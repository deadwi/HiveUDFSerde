package example.hive.serde;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.apache.log4j.Logger;

public class SchemaLogSerde extends AbstractSerDe
{
	private static final int FIELD_INDEX_SCHEMA = 1;
  private static final int FIELD_INDEX_VERSION = 2;

  private StructTypeInfo rowTypeInfo;
	private ObjectInspector rowOI;
	private List<String> colNames;
	private List<TypeInfo> colTypes;
	private List<Object> row;
	private Map<String, int[]> schemaMap = new HashMap<String, int[]>();
	private String schemaName;
	
	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException
	{
		String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
		colNames = Arrays.asList(colNamesStr.split(","));
		for (int i = 0; i < colNames.size(); i++)
			colNames.set(i, colNames.get(i).toLowerCase());

		String colTypesStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
		colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
		rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
		rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);

		row = new ArrayList<Object>(colNames.size());
		for (int c = 0; c < colNames.size(); c++)
			row.add(null);

		schemaName = tbl.getProperty(Names.SCHEMA_NAME);
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException
	{
		if(schemaMap.isEmpty())
      loadSchemaInfo();

		for (int c = 0; c < row.size(); c++)
			row.set(c, null);

		Text rowText = (Text) blob;
		String[] fieldSet = rowText.toString().split(",");
		if (fieldSet.length < FIELD_INDEX_VERSION)
      return null;
    if (schemaName.compareToIgnoreCase(fieldSet[FIELD_INDEX_SCHEMA])!=0)
      return null;

		int[] indexMap = schemaMap.get(fieldSet[FIELD_INDEX_VERSION]);
		if (indexMap == null)
			return null;

		for (int i = 0; i < indexMap.length; i++) {
			int c = indexMap[i];
			if (c < 0)
				continue;
			String v = i < fieldSet.length ? fieldSet[i] : null;
			row.set(c, getTypeObjectFromString(v, colTypes.get(c)));
		}
		return row;
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
		throw new UnsupportedOperationException("SchemaLogSerde doesn't support the serialize() method");
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

	private void loadSchemaInfo() throws SerDeException
	{
    schemaMap.clear();

    // get schema version and field list from meta store (like DB)
    String fieldv1[] = {"datetime", "schema", "version", "level", "money"};
    String fieldv2[] = {"datetime", "schema", "version", "level", "exp", "money"};
    addSchemaInfo("v1", fieldv1);
    addSchemaInfo("v2", fieldv2);

		if(schemaMap.isEmpty())
			throw new SerDeException(schemaName+" has not schema in Metastore");
	}

  private void addSchemaInfo(String version, String[] fieldList) {
    int[] indexMapping = new int[fieldList.length];
    for (int i = 0; i < fieldList.length; i++)
      indexMapping[i] = colNames.indexOf(fieldList[i].toLowerCase());
    schemaMap.put(version, indexMapping);
  }
}
