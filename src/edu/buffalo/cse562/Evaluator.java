package edu.buffalo.cse562;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Evaluator implements ExpressionVisitor
{
	// CLASS VARIABLES

	Column[] schema;
	Object[] tuple;
	Column[] coldataType;

	String colDataType;

	String columnDataString;
	boolean bool = true;
	int ii = 0;

	HashMap<String,Integer> columnMap = new HashMap<String,Integer>();
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

	// CONSTRUCTOR FOR WHERE CLAUSE AND JOIN CONDITION

	public Evaluator(Column[] schema, HashMap<String,Integer> columnMap ,Column[] coldataType) 
	{
		this.schema = schema;
		this.columnMap = columnMap;
		this.coldataType = coldataType;
	}
	
	public void sendTuple(Object[] tuple)
	{
		this.tuple = tuple;
	}


	public void visit(NullValue expression) 
	{
		System.out.println("NullValue" + " " + ii++);
	}


	public void visit(Function expression) 
	{
		//System.out.println("Function" + " " + ii++);
		if (expression.getName().equalsIgnoreCase("DATE"))
		{
			columnDataString = expression.getParameters().toString();
			columnDataString = columnDataString.substring(2, 12);
		}
	}


	public void visit(InverseExpression expression) 
	{
		System.out.println("InverseExpression" + " " + ii++);
	}


	public void visit(JdbcParameter expression) 
	{
		System.out.println("JdbcParameter" + " " + ii++);
	}


	public void visit(DoubleValue expression) 
	{
		//System.out.println("DoubleValue" + " " + ii++);
		Double value;
		value = expression.getValue();
		columnDataString = String.valueOf(value);
	}


	public void visit(LongValue expression) 
	{
		//System.out.println("LongValue" + " " + ii++);
		columnDataString = expression.getStringValue();
	}


	public void visit(DateValue expression) 
	{
		System.out.println("DateValue" + " " + ii++);
	}


	public void visit(TimeValue expression) 
	{
		System.out.println("TimeValue" + " " + ii++);
	}


	public void visit(TimestampValue expression) 
	{
		System.out.println("TimestampValue" + " " + ii++);
	}


	public void visit(Parenthesis expression) 
	{
		//System.out.println("Parenthesis" + " " + ii++);
		expression.getExpression().accept(this);
	}


	public void visit(StringValue expression) 
	{
		//System.out.println("StringValue" + " " + ii++);
		columnDataString = expression.getValue();
	}


	public void visit(Addition expression) 
	{
		//System.out.println("Addition" + " " + ii++);
		Double columnData;
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;

		columnData = Double.parseDouble(leftData) + Double.parseDouble(rightData);
		columnDataString = String.valueOf(columnData);

	}


	public void visit(Division expression) 
	{
		//System.out.println("Division" + " " + ii++);
		Double columnData;
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;

		columnData = Double.parseDouble(leftData) / Double.parseDouble(rightData);
		columnDataString = String.valueOf(columnData);

	}


	public void visit(Multiplication expression) 
	{
		//System.out.println("Multiplication" + " " + ii++);
		Double columnData;
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;

		columnData = Double.parseDouble(leftData) * Double.parseDouble(rightData);
		columnDataString = String.valueOf(columnData);

	}


	public void visit(Subtraction expression) 
	{
		//System.out.println("Subtraction" + " " + ii++);
		Double columnData;
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;

		columnData = Double.parseDouble(leftData) - Double.parseDouble(rightData);
		columnDataString = String.valueOf(columnData);

	}


	public void visit(AndExpression expression) 
	{
		//System.out.println("AndExpression" + " " + ii++);
		boolean leftBool;
		boolean rightBool;
		expression.getLeftExpression().accept(this);
		leftBool = bool;
		expression.getRightExpression().accept(this);
		rightBool = bool;
		if (leftBool == false || rightBool == false)
			bool = false;
		else
			bool = true;
	}


	public void visit(OrExpression expression) 
	{
		//System.out.println("OrExpression" + " " + ii++);
		boolean leftBool;
		boolean rightBool;
		expression.getLeftExpression().accept(this);
		leftBool = bool;
		expression.getRightExpression().accept(this);
		rightBool = bool;
		if (leftBool == true || rightBool == true)
			bool = true;
		else
			bool = false;
	}


	public void visit(Between expression) 
	{
		System.out.println("Between" + " " + ii++);
	}


	public void visit(EqualsTo expression) 
	{
		//System.out.println("EqualsTo" + " " + ii++);
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;

		if (colDataType.toUpperCase().contains("CHAR"))
		{
			if (leftData.equals(rightData))
				bool = true;
			else
				bool = false;
		}
		else if (colDataType.toUpperCase().contains("DATE"))
		{
			try 
			{
				if (dateFormat.parse(leftData).equals(dateFormat.parse(rightData)))
					bool = true;
				else
					bool = false;
			}
			catch (Exception e)
			{
				
			}
		}
		else
		{
			if (Double.parseDouble(leftData) == Double.parseDouble(rightData))
				bool = true;
			else
				bool = false;
		}
	}


	public void visit(GreaterThan expression) 
	{
		//System.out.println("GreaterThan" + " " + ii++);
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;

		if (colDataType.toUpperCase().contains("DATE"))
		{
			try 
			{
				if (dateFormat.parse(leftData).equals(dateFormat.parse(rightData)))
					bool = true;
				else
					bool = false;
			}
			catch (Exception e)
			{
				
			}
		}
		else
		{
			if (Double.parseDouble(leftData) > Double.parseDouble(rightData))
				bool = true;
			else
				bool = false;
		}
		

	}


	public void visit(GreaterThanEquals expression) 
	{
		//System.out.println("GreaterThanEquals" + " " + ii++);
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;

		if (colDataType.toUpperCase().contains("DATE"))
		{
			try 
			{
				if (!(dateFormat.parse(leftData).before(dateFormat.parse(rightData))))
					bool = true;
				else
					bool = false;
			}
			catch (Exception e)
			{
				
			}
		}
		else
		{
			if (Double.parseDouble(leftData) >= Double.parseDouble(rightData))
				bool = true;
			else
				bool = false;
		}
		
		
	}


	public void visit(InExpression expression) 
	{
		System.out.println("InExpression" + " " + ii++);
	}


	public void visit(IsNullExpression expression) 
	{
		System.out.println("IsNullExpression" + " " + ii++);
	}


	public void visit(LikeExpression expression) 
	{
		//System.out.println("LikeExpression" + " " + ii++);
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);;
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;


		if (rightData.contains("%"))
		{
			if (rightData.startsWith("%"))
			{
				rightData = rightData.replace("%", "");
				if (leftData.endsWith(rightData))
					bool = true;
				else
					bool = false;
			}
			if (rightData.endsWith("%"))
			{
				rightData = rightData.replace("%", "");
				if (leftData.startsWith(rightData))
					bool = true;
				else
					bool = false;
			}
		}

	}


	public void visit(MinorThan expression) 
	{
		//System.out.println("MinorThan" + " " + ii++);
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;
		
		if (colDataType.toUpperCase().contains("DATE"))
		{
			try 
			{
				if (dateFormat.parse(leftData).before(dateFormat.parse(rightData)))
					bool = true;
				else
					bool = false;
			}
			catch (Exception e)
			{
				
			}
		}
		else
		{
			if (Double.parseDouble(leftData) < Double.parseDouble(rightData))
				bool = true;
			else
				bool = false;
		}
		
	}


	public void visit(MinorThanEquals expression) 
	{
		//System.out.println("MinorThanEquals" + " " + ii++);
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;
		
		if (colDataType.toUpperCase().contains("DATE"))
		{
			try 
			{
				if (!(dateFormat.parse(leftData).after(dateFormat.parse(rightData))))
					bool = true;
				else
					bool = false;
			}
			catch (Exception e)
			{
				
			}
		}
		else
		{
			if (Double.parseDouble(leftData) <= Double.parseDouble(rightData))
				bool = true;
			else
				bool = false;
		}
		
	}


	public void visit(NotEqualsTo expression) 
	{
		//System.out.println("NotEqualsTo" + " " + ii++);
		String leftData;
		String rightData;
		expression.getLeftExpression().accept(this);
		leftData = columnDataString;
		expression.getRightExpression().accept(this);
		rightData = columnDataString;

		if (colDataType.toUpperCase().contains("CHAR"))
		{
			if (!(leftData.equals(rightData)))
				bool = true;
			else
				bool = false;
		}
		else if (colDataType.toUpperCase().contains("DATE"))
		{
			try 
			{
				if (!(dateFormat.parse(leftData).equals(dateFormat.parse(rightData))))
					bool = true;
				else
					bool = false;
			}
			catch (Exception e)
			{
				
			}
		}
		else
		{
			if (!(Double.parseDouble(leftData) == Double.parseDouble(rightData)))
				bool = true;
			else
				bool = false;
		}

	}


	public void visit(Column expression) 
	{
		int index;
		
		if (expression.toString().contains("."))
		{
			index = columnMap.get(expression.toString().toLowerCase().split("\\.")[1]);
		}
		else
		{
			index = columnMap.get(expression.toString().toLowerCase());
		}
		columnDataString = (String) tuple[index];
		colDataType = coldataType[index].getColumnName();
	}


	public void visit(SubSelect expression) 
	{
		
	}


	public void visit(CaseExpression expression) 
	{
		
	}


	public void visit(WhenClause expression) 
	{
		
	}


	public void visit(ExistsExpression expression) 
	{
		
	}


	public void visit(AllComparisonExpression expression) 
	{
		
	}


	public void visit(AnyComparisonExpression expression) 
	{
		
	}


	public void visit(Concat expression) 
	{
		
	}


	public void visit(Matches expression) 
	{
		
	}


	public void visit(BitwiseAnd expression) 
	{
		
	}


	public void visit(BitwiseOr expression) 
	{
		
	}


	public void visit(BitwiseXor expression) 
	{
		
	}

	public boolean getBool() 
	{
		return bool;
	}
}

