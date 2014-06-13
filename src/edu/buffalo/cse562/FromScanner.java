package edu.buffalo.cse562;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class FromScanner implements FromItemVisitor
{
	// CLASS VARIABLES

	File dataDir;
	HashMap<String, CreateTable> tables = new HashMap<String, CreateTable>();

	File swapDir;
	public Column[] schema = null;
	public Column[] coldataType = null;
	
	public HashMap<String,Integer> columnMap = new HashMap<String,Integer>();
	public String tableMap;
	public String aliasMap = "";
	public File source = null;
	public File indexDir;
	public List<String[]> subList;
	Boolean flag = false;


	// CONSTRUCTOR

	public FromScanner(File dataDir,File swapDir, File indexDir, HashMap<String, CreateTable> tables)
	{
		this.dataDir = dataDir;
		this.tables = tables;
		this.swapDir = swapDir;
		this.indexDir = indexDir;
	}


	@Override
	public void visit(Table tableName) 
	{
		CreateTable table = tables.get(tableName.getName().toUpperCase());
	
		List columnDef = table.getColumnDefinitions();
		schema = new Column[columnDef.size()];
		coldataType = new Column[columnDef.size()];
		
        tableMap = tableName.toString();
        
        if(tableName.getAlias() != null)
        {
        	aliasMap = tableName.getAlias().toString();
        }
        
		for (int i = 0 ; i < columnDef.size() ; i++)
		{
			ColumnDefinition col = (ColumnDefinition) columnDef.get(i);
			schema[i] = new Column(tableName , col.getColumnName());
			coldataType[i] = new Column(tableName, col.getColDataType().toString());
			columnMap.put(col.getColumnName(), i);
		}

		source = new File(dataDir , tableName.getName().toLowerCase() + ".dat");
	}

	@Override
	public void visit(SubSelect subSelect) 
	{
		CCJSqlParser parser = new CCJSqlParser(new StringReader(subSelect.getSelectBody().toString()));
		Statement parsedStatement;
		try 
		{
			parsedStatement = parser.Statement();
			if (parsedStatement instanceof Select)
			{
				Select subSelectStatement = (Select) parsedStatement;
				SubSelectOperator subSelectOperator = new SubSelectOperator(dataDir, swapDir, indexDir,  tables);
				subSelectOperator.initializeSelect(subSelectStatement);

				parser = new CCJSqlParser(new StringReader(subSelect.getAlias().toString()));
				Table tableName = (Table) parser.Table();
				schema = new Column[subSelectOperator.getSchemaList().size()];

				for (int i = 0 ; i < subSelectOperator.getSchemaList().size() ; i++)
				{
					schema[i] = new Column(tableName , subSelectOperator.getSchemaAliasList().get(i));	
				}
				
				flag = true;
				subList = subSelectOperator.getSubFile();
				
			}
		} 
		catch (ParseException e) 
		{
			e.printStackTrace();
		}
	}

	@Override
	public void visit(SubJoin subJoin) 
	{

	}

}
