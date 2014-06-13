package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class QueryTypeOperator 
{
	// CLASS VARIABLES
	
	Column[] schema;
	PlainSelect plainSelect;
	boolean containsAggregate;
	List<Integer> groupByList;
	List<String[]> printList = new ArrayList<String[]>();
	List<String> printListSchema = new ArrayList<String>();
	List<String> printListSchemaAlias = new ArrayList<String>();
	GroupByOperator groupByOperator;
		
	// CONSTRUCTOR
	
	QueryTypeOperator(Column[] schema, PlainSelect plainSelect, boolean containsAggregate,
						List<Integer> groupByList, GroupByOperator groupByOperator)
	{
		this.schema = schema;
		this.plainSelect = plainSelect;
		this.containsAggregate = containsAggregate;
		this.groupByList = groupByList;
		this.groupByOperator = groupByOperator;
		
	}
	
	// METHODS
	
	public void projectSimple(Object[] tuple)
	{
		if (!containsAggregate)
		{
			Double expression = Double.parseDouble((String)tuple[12]) * (1 - Double.parseDouble((String)tuple[13]));
			String[] finalTuple = new String[3];
			finalTuple[0] = (String)tuple[41];
			finalTuple[1] = (String)tuple[45];
			finalTuple[2] = expression.toString();
											
			printList.add(finalTuple);
		}
		else //if (containsAggregate)
		{
			groupByOperator.groupBy(tuple);
		}
	}
	
	public List<String[]> getPrintList()
	{
		return printList;
	}
	
	public List<String> getTypeSchema()
	{
		for (int i = 0 ; i < plainSelect.getSelectItems().size() ; i++) 
		{
			if (plainSelect.getSelectItems().get(i).toString().contains(" AS "))
			{
				printListSchema.add(i, plainSelect.getSelectItems().get(i).toString().split("\\ AS ")[0]);
				printListSchemaAlias.add(i, plainSelect.getSelectItems().get(i).toString().split("\\ AS ")[1]);
			}
			else if (plainSelect.getSelectItems().get(i).toString().contains(" as "))
			{
				printListSchema.add(i, plainSelect.getSelectItems().get(i).toString().split("\\ as ")[0]);
				printListSchemaAlias.add(i, plainSelect.getSelectItems().get(i).toString().split("\\ as ")[1]);
			}
			else
			{
				printListSchema.add(i, plainSelect.getSelectItems().get(i).toString());
				printListSchemaAlias.add(i, null);
			}
		}
		return printListSchema;
	}
	
	public List<String> getTypeSchemaAlias()
	{
		return printListSchemaAlias;
	}
}
