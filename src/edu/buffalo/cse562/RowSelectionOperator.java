package edu.buffalo.cse562;

import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class RowSelectionOperator implements Operator
{
	// CLASS VARIABLES
	
	Operator input;
	Column[] schema;
	String tableMap;
	String aliasMap;
	HashMap<String,Integer> columnMap = new HashMap<String,Integer>();
	Expression condition;
	Column[] coldataType;
		
	// CONSTRUCTOR
	
	public RowSelectionOperator(Operator input, Column[] schema, Expression condition, HashMap<String,Integer> columnMap, String tableMap, String aliasMap, Column[] coldataType)
	{
		this.input = input;
		this.schema = schema;
		this.condition = condition;
		this.columnMap = columnMap;
		this.tableMap = tableMap;
		this.aliasMap = aliasMap;
		this.coldataType = coldataType;
	}
		
	public String[] readOneTuple() 
	{
		Evaluator eval;
		String[] tuple = null;
		
		do
		{
			tuple = input.readOneTuple();
			
			if (tuple == null)
			{
				return null;
			}
			
			if (condition != null)
			{
				eval = new Evaluator(schema, columnMap,coldataType);
				condition.accept(eval);
				if (!eval.getBool())
				{
					tuple = null;
				}
			}
						
		} 
		while (tuple == null);
		
		return tuple;
	}

	public void reset() 
	{
		input.reset();
	}

	@Override
	public String line() 
	{
		return input.line();
	}

}
