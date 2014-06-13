package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.update.Update;

public class UpdateOperator 
{
	File dataDir;
	File swapDir;
	File indexDir;
	HashMap<String, CreateTable> tables;
	RecordManager rec;
	Boolean debugMode;
	HashMap<String,Integer> schema = new HashMap<String,Integer>();
	Column[] dataType;
	Column[] schemaCol;
		
	
	public UpdateOperator(File dataDir, File swapDir, File indexDir, HashMap<String, CreateTable> tables, RecordManager rec, Boolean debugMode) 
	{
		this.dataDir = dataDir;
		this.swapDir = swapDir;
		this.indexDir = indexDir;
		this.tables = tables;
		this.rec = rec;
		this.debugMode = debugMode;
	}

	public void initializeUpdate(Update updateStatement) 
	{
		String tableName = updateStatement.getTable().getName().toUpperCase();
		schemaFinder(tableName);
		
		List columnsToBeChanged = updateStatement.getColumns();
		List dataForTheChange = updateStatement.getExpressions();
		Expression whereClause = updateStatement.getWhere();
		
		for (int index = 0 ; index < 2 ; index ++)
		{
			if (tableName.equalsIgnoreCase("lineitem") && index > 0)
			{
				break;
			}
						
			PrimaryTreeMap<String,List<String[]>> primaryMap = rec.treeMap(tableName.toLowerCase() + "_" + index);
			
			Evaluator eval = new Evaluator(schemaCol, schema, dataType);
			
			for (int i = 0 ; i < columnsToBeChanged.size() ; i++)
			{
				int indexToBeChanged = schema.get(columnsToBeChanged.get(i).toString().toLowerCase());
				String data = dataForTheChange.get(i).toString().replace("\'", "");
							
				for (List<String[]> keyList : primaryMap.values())
				{
					Object[] key1 = keyList.get(0);
					List<String[]> tempList = new ArrayList<String[]>();
					Boolean check = false; 
					for (int j = 0 ; j < keyList.size() ; j++)
					{
						Object[] key = keyList.get(j);
						eval.sendTuple(key);
						whereClause.accept(eval);
									
						if (eval.getBool())
						{
							check = true;
							String[] keyNew = new String[key.length];
							for (int k = 0 ; k < key.length ; k++)
							{
								if (indexToBeChanged == k)
								{
									keyNew[k] = data;
								}
								else
								{
									keyNew[k] = (String)key[k];
								}
							}
							tempList.add(keyNew);
						}
						else
						{
							tempList.add(Arrays.asList(key).toArray(new String[key.length]));
						}
					}
					if (check)
					{
						primaryMap.put((String)key1[index], tempList);
					}
				}
			}
		}
	}
	
	public void schemaFinder(String tableName)
	{
		CreateTable cTab = tables.get(tableName);
		List columnDef = cTab.getColumnDefinitions();
		Table table = cTab.getTable();
		schemaCol = new Column[columnDef.size()];
		dataType = new Column[columnDef.size()];
				        
		for (int i = 0 ; i < columnDef.size() ; i++)
		{
			ColumnDefinition col = (ColumnDefinition) columnDef.get(i);
			schema.put(col.getColumnName(),i);
			schemaCol[i] = new Column(table, col.getColumnName());
			dataType[i] = new Column(table, col.getColDataType().toString());
			
		}
	}
}
