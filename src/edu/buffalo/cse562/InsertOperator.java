package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SubSelect;


public class InsertOperator implements ItemsListVisitor
{
	// CLASS VARIABLES

	public File dataDir;
	public File swapDir;
	public File indexDir;
	public HashMap<String, CreateTable> tables;
	String tableName;
	RecordManager rec;
	Boolean debugMode;

	// CONSTRUCTOR FOR CLASS

	public InsertOperator(File dataDir, File swapDir, File indexDir, HashMap<String, CreateTable> tables, RecordManager rec, Boolean debugMode) 
	{
		this.dataDir = dataDir;
		this.swapDir = swapDir;
		this.indexDir = indexDir;
		this.tables = tables;
		this.rec = rec;
		this.debugMode = debugMode;
	}

	// STARTING THE Insert STATEMENT EVALUATION PROCESS

	public void initializeInsert(Insert insertStatement) 
	{
		tableName = insertStatement.getTable().getName().toLowerCase();
		ItemsList rawItems = insertStatement.getItemsList();
		
		if (debugMode)
			System.out.println("Inserting into " + tableName + " Table");
		
		rawItems.accept(this);
	}

	public void visit(ExpressionList insertValues) 
	{
		List values = insertValues.getExpressions();
		String[] tupleToInsert = new String[values.size()];
		for (int i = 0 ; i < values.size() ; i++)
		{
			tupleToInsert[i] = values.get(i).toString().replaceAll("\'", "").toString().replace("date(", "").replace(")","");
			if (debugMode)
				System.out.print(tupleToInsert[i]+"|");
		}
		
		if (debugMode)
			System.out.println();
		
		for (int i = 0 ; i < values.size(); i++)
		{
			if (i < 2)
			{
				if (debugMode)
					System.out.println("Inserting into Index" + i);
				addIndex(i, tupleToInsert);
			}
		}
	}


	public void addIndex(int columnNumber, String[] s)
	{
		try
		{
			PrimaryTreeMap<String,List<String[]>> primaryTreeMap = rec.treeMap(tableName+"_"+columnNumber);

			if (primaryTreeMap.containsKey(s[columnNumber]))
			{
				primaryTreeMap.get(s[columnNumber]).add(s);
			}
			else
			{
				List<String[]> temp = new ArrayList<String[]>();
				temp.add(s);
				primaryTreeMap.put(s[columnNumber], temp);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}



	public void visit(SubSelect arg0) 
	{}
}
