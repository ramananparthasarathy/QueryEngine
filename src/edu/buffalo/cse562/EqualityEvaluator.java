package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

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

public class EqualityEvaluator implements ExpressionVisitor
{
	// CLASS VARIABLES

	Column[] schema;
	
	String columnDataString = "notinschema";
	String tableName;
	List<Integer> indexList;
	boolean columnBool = false;
	boolean equalityBool = false;
	int indexer = 0;

	// CONSTRUCTOR FOR WHERE CLAUSE AND JOIN CONDITION

	public EqualityEvaluator(Column[] schema) 
	{
		this.schema = schema;
		indexList = new ArrayList<Integer>();
	}


	public void visit(NullValue expression) 
	{
		columnBool = false;
	}


	public void visit(Function expression) 
	{
		columnBool = false;
	}


	public void visit(InverseExpression expression) 
	{
		columnBool = false;
	}


	public void visit(JdbcParameter expression) 
	{
		columnBool = false;
	}


	public void visit(DoubleValue expression) 
	{
		columnBool = false;
	}


	public void visit(LongValue expression) 
	{
		columnBool = false;
	}


	public void visit(DateValue expression) 
	{
		columnBool = false;
	}


	public void visit(TimeValue expression) 
	{
		columnBool = false;
	}


	public void visit(TimestampValue expression) 
	{
		columnBool = false;
	}


	public void visit(Parenthesis expression) 
	{
		columnBool = false;
	}


	public void visit(StringValue expression) 
	{
		columnBool = false;
	}


	public void visit(Addition expression) 
	{
		columnBool = false;
	}


	public void visit(Division expression) 
	{
		columnBool = false;
	}


	public void visit(Multiplication expression) 
	{
		columnBool = false;
	}


	public void visit(Subtraction expression) 
	{
		columnBool = false;
	}


	public void visit(AndExpression expression) 
	{
		expression.getLeftExpression().accept(this);
		
		expression.getRightExpression().accept(this);
	}


	public void visit(OrExpression expression) 
	{
		expression.getLeftExpression().accept(this);
		
		expression.getRightExpression().accept(this);
	}


	public void visit(Between expression) 
	{
		equalityBool = false;
	}


	public void visit(EqualsTo expression) 
	{
		equalityBool = true;
		expression.getLeftExpression().accept(this);
		boolean leftBool = columnBool;
		String leftData = columnDataString;
		int leftIndex = indexer;
		columnBool = false;
		expression.getRightExpression().accept(this);
		boolean rightBool = columnBool;
		String rightData = columnDataString;
		int rightIndex = indexer;
		columnBool = false;
				
		if (leftBool == true && rightBool == true)
		{
			if (!leftData.equals("notinschema"))
			{
				indexList.add(leftIndex);
			}
			if (!rightData.equals("notinschema"))
			{
				indexList.add(rightIndex);
			}
		}
	}


	public void visit(GreaterThan expression) 
	{
		equalityBool = false;
	}


	public void visit(GreaterThanEquals expression) 
	{
		equalityBool = false;
	}


	public void visit(InExpression expression) 
	{
		equalityBool = false;
	}


	public void visit(IsNullExpression expression) 
	{
		equalityBool = false;
	}


	public void visit(LikeExpression expression) 
	{
		equalityBool = false;
	}


	public void visit(MinorThan expression) 
	{
		equalityBool = false;
	}


	public void visit(MinorThanEquals expression) 
	{
		equalityBool = false;
	}


	public void visit(NotEqualsTo expression) 
	{
		equalityBool = false;
	}


	public void visit(Column expression) 
	{
		columnBool = true;

		int index = 900;

		for (int i = 0 ; i < schema.length ; i++)
		{
			if (expression.toString().contains("."))
			{
				if (expression.toString().equals(schema[i].getTable().getAlias()+"."+schema[i].getColumnName()) 
						|| expression.toString().equals(schema[i].getTable().getName()+"."+schema[i].getColumnName()))
				{
					index = i;
					break;
				}
			}
			else
			{
				if (expression.toString().equals(schema[i].getColumnName()))
				{
					index = i;
					break;
				}
			}
		}
		if (index != 900)
		{
			columnDataString = "inschema";
			tableName = schema[index].getTable().getName();
			indexer = index;
		}
		else
		{
			columnDataString = "notinschema";
		}
	}


	public void visit(SubSelect expression) 
	{
		columnBool = false;
	}


	public void visit(CaseExpression expression) 
	{
		columnBool = false;
	}


	public void visit(WhenClause expression) 
	{
		columnBool = false;
	}


	public void visit(ExistsExpression expression) 
	{
		columnBool = false;
	}


	public void visit(AllComparisonExpression expression) 
	{
		columnBool = false;
	}


	public void visit(AnyComparisonExpression expression) 
	{
		columnBool = false;
	}


	public void visit(Concat expression) 
	{
		columnBool = false;
	}


	public void visit(Matches expression) 
	{
		columnBool = false;
	}


	public void visit(BitwiseAnd expression) 
	{
		columnBool = false;
	}


	public void visit(BitwiseOr expression) 
	{
		columnBool = false;
	}


	public void visit(BitwiseXor expression) 
	{
		columnBool = false;
	}
	
	public List<Integer> getIndexList()
	{
		return indexList;
	}
	
	public String getTableName()
	{
		return tableName;
	}

}
