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
import net.sf.jsqlparser.statement.delete.Delete;

public class DeleteOperator 
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


	public DeleteOperator(File dataDir, File swapDir, File indexDir, HashMap<String, CreateTable> tables, RecordManager rec, Boolean debugMode) 
	{
		this.dataDir = dataDir;
		this.swapDir = swapDir;
		this.indexDir = indexDir;
		this.tables = tables;
		this.rec = rec;
		this.debugMode = debugMode;
	}

	public void initializeUpdate(Delete deleteStatement) 
	{
		String tableName = deleteStatement.getTable().getName().toUpperCase();
		schemaFinder(tableName);

		Expression whereClause = deleteStatement.getWhere();
		
		if (debugMode)
			System.out.println("Deleting from " + tableName);
		
		for (int index = 0 ; index < 2 ; index ++)
		{
			if (tableName.equalsIgnoreCase("lineitem") && index > 0)
			{
				break;
			}
			
			PrimaryTreeMap<String,List<String[]>> primaryMap = rec.treeMap(tableName.toLowerCase() + "_" + index);
	
			Evaluator eval = new Evaluator(schemaCol, schema, dataType);
	
			for (List<String[]> keyList : primaryMap.values())
			{
				Object[] key1 = keyList.get(0);
				Boolean check = false;
				List<String[]> tempList = new ArrayList<String[]>();
				for (int j = 0 ; j < keyList.size() ; j++)
				{
					Object[] key = keyList.get(j);
					eval.sendTuple(key);
					whereClause.accept(eval);
	
					if (eval.getBool())
					{
						if (debugMode)
						{
							System.out.println("DELETING IN "+key1[0]);
							for (int x = 0 ; x < key.length ; x++)
							{
								System.out.print(key[x] + "|");
							}
							System.out.println();
						}
					}
					else
					{
						check = true;
						tempList.add(Arrays.asList(key).toArray(new String[key.length]));
					}
				}
				if (check)
				{
					
					primaryMap.put((String)key1[index], tempList);
				}
				else
				{
					primaryMap.remove((String)key1[index]);
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
