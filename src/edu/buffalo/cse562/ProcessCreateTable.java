package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class ProcessCreateTable 
{
	File dataDir;
	File swapDir;
	File indexDir;
	Boolean debugMode;

	public ProcessCreateTable(File dataDir, File swapDir, File indexDir, Boolean debugMode)
	{
		this.dataDir = dataDir;
		this.swapDir = swapDir;
		this.indexDir = indexDir;
		this.debugMode = debugMode;
	}

	protected void startProcess(CreateTable cTab)
	{
		String tableName = cTab.getTable().getName().toLowerCase();
		
		if (debugMode)
			System.out.println("Sorting DataFile : "+ tableName + ".dat");
		
		// SORT THE FILE HERE
		FileSortOperator fs = new FileSortOperator(tableName, new File(dataDir, tableName+".dat"), swapDir);
		fs.sort();
		
		if (debugMode)
			System.out.println("Indexing : "+ tableName + ".dat");
		
		int noOfColumns = cTab.getColumnDefinitions().size();
		for (int i = 0 ; i < noOfColumns ; i++)
		{
			if (i < 2)
			{
				if (debugMode)
					System.out.println("Creating Index on Column " + i);
				
				makeIndex(tableName, i);
			}
		}
		if (tableName.equalsIgnoreCase("customer"))
		{
			makeIndex(tableName, 3);
		}

	}


	public void makeIndex(String tableName, int columnNumber)
	{
		try
		{
			RecordManager rec = RecordManagerFactory.createRecordManager((new File(indexDir, "index.db")).toString());
			PrimaryTreeMap<String,List<String[]>> primaryTreeMap = rec.treeMap(tableName+"_"+columnNumber);
			Scanner scanner = new Scanner(new File(swapDir, tableName+".dat"));
			String eachLine = null;
			while (scanner.hasNextLine())
			{
				eachLine = scanner.nextLine();
				String [] breakLine = eachLine.split("\\|");
				
				if (primaryTreeMap.containsKey(breakLine[columnNumber]))
				{
					primaryTreeMap.get(breakLine[columnNumber]).add(breakLine);
				}
				else
				{
					List<String[]> temp = new ArrayList<String[]>();
					temp.add(breakLine);
					primaryTreeMap.put(breakLine[columnNumber], temp);
				}

			}
			scanner.close();
			rec.commit();
			rec.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}