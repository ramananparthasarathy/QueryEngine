package edu.buffalo.cse562;

import java.io.*;
import java.util.*;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

public class Main 
{
	// TIME FUNCTIONS

	public static long time()
	{
		return System.nanoTime();
	}

	// MAIN METHOD

	public static void main(String[] args) throws Exception
	{
		//System.gc();
		long start = time();

		// ACTUAL FUNCTIONS

		File dataDir = null;
		File swapDir = null;
		File indexDir = null;
		Boolean buildFlag = false;
		Boolean timeFlag = false;
		Boolean debugMode = false;

		List<File> sqlFiles = new ArrayList<File>();
		HashMap<String,CreateTable> tables = new HashMap<String,CreateTable>();

		// GO THROUGH THE ARGUEMENTS AND CREATE DIR, FLAGS & FILES AS NEEDED

		for (int i = 0 ; i < args.length ; i++)
		{
			if (args[i].equalsIgnoreCase("--data"))
			{
				dataDir = new File(args[i+1]);
				i++;
			}
			else if (args[i].equalsIgnoreCase("--swap"))
			{
				swapDir = new File(args[i+1]);
				i++;
			}
			else if (args[i].equalsIgnoreCase("--index"))
			{
				indexDir = new File(args[i+1]);
				i++;
			}
			else if (args[i].equalsIgnoreCase("--build"))
			{
				buildFlag = true;
			}
			else if (args[i].equalsIgnoreCase("--time"))
			{
				timeFlag = true;
			}
			else if (args[i].equalsIgnoreCase("--debug"))
			{
				debugMode = true;
			}
			else
			{
				sqlFiles.add(new File(args[i]));
			}
		}

		if (buildFlag)
		{
			if (debugMode)
			{
				for(File file: indexDir.listFiles())
				{
					System.out.println("Deleting Previous Index File -> " + file);
					file.delete();
				}
			}

			ProcessCreateTable pCT = new ProcessCreateTable(dataDir, swapDir, indexDir, debugMode);

			for (File sql : sqlFiles)
			{
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement parsedStatement;

				while ((parsedStatement = parser.Statement()) != null)
				{
					if (parsedStatement instanceof CreateTable)
					{
						CreateTable createStatement = (CreateTable) parsedStatement;
						tables.put(createStatement.getTable().getName(), createStatement);
						pCT.startProcess(createStatement);
					}
				}
			}
		}
		else
		{
			RecordManager rec = RecordManagerFactory.createRecordManager((new File(indexDir, "index.db")).toString());

			for (File sql : sqlFiles)
			{
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement parsedStatement;

				while ((parsedStatement = parser.Statement()) != null)
				{
					if (parsedStatement instanceof CreateTable)
					{
						CreateTable createStatement = (CreateTable) parsedStatement;
						tables.put(createStatement.getTable().getName(), createStatement);
					}
					else if (parsedStatement instanceof Select)
					{
						rec.commit();
						rec.close();
						Select selectStatement = (Select)parsedStatement;
						SelectOperator selectOperator = new SelectOperator(dataDir, swapDir, indexDir, tables);
						selectOperator.initializeSelect(selectStatement);
					}
					else if (parsedStatement instanceof Insert)
					{
						Insert insertStatement = (Insert)parsedStatement;
						InsertOperator insertOperator = new InsertOperator(dataDir, swapDir, indexDir, tables, rec, debugMode);
						insertOperator.initializeInsert(insertStatement);
					}
					else if (parsedStatement instanceof Update)
					{
						Update updateStatement = (Update)parsedStatement;
						UpdateOperator updateOperator = new UpdateOperator(dataDir, swapDir, indexDir, tables, rec, debugMode);
						updateOperator.initializeUpdate(updateStatement);
					}
					else if (parsedStatement instanceof Delete)
					{
						Delete deleteStatement = (Delete)parsedStatement;
						DeleteOperator deleteOperator = new DeleteOperator(dataDir, swapDir, indexDir, tables, rec, debugMode);
						deleteOperator.initializeUpdate(deleteStatement);
					}
					else
					{
						System.out.println("Statement not supported by the program yet !!!");
					}
				}
			}
		}

		long end = time();
		if (timeFlag)
		{
			System.out.println();
			System.out.println("TIME TAKEN = " + (end - start)/1e9);
		}
	}
}
