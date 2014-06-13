package edu.buffalo.cse562;

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

public class AggregateEvaluator implements ExpressionVisitor
{
	// CLASS VARIABLES

	Column[] schema;
	Object[] tuple;

	String columnDataString;
	int ii = 0;

	// CONSTRUCTOR FOR WHERE CLAUSE AND JOIN CONDITION

	public AggregateEvaluator(Column[] schema, Object[] tuple) 
	{
		this.tuple = tuple;
		this.schema = schema;
	}


	public void visit(NullValue expression) 
	{
		System.out.println("NullValue" + " " + ii++);
	}


	public void visit(Function expression) 
	{
				
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
		System.out.println("DoubleValue" + " " + ii++);
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
		expression.getExpression().accept(this);
	}

	public void visit(StringValue expression) 
	{
		System.out.println("StringValue" + " " + ii++);
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
		System.out.println("AndExpression" + " " + ii++);
	}


	public void visit(OrExpression expression) 
	{
		System.out.println("OrExpression" + " " + ii++);
	}


	public void visit(Between expression) 
	{
		System.out.println("Between" + " " + ii++);
	}


	public void visit(EqualsTo expression) 
	{
		System.out.println("EqualsTo" + " " + ii++);
	}


	public void visit(GreaterThan expression) 
	{
		System.out.println("GreaterThan" + " " + ii++);
	}


	public void visit(GreaterThanEquals expression) 
	{
		System.out.println("GreaterThanEquals" + " " + ii++);
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
		System.out.println("LikeExpression" + " " + ii++);
	}


	public void visit(MinorThan expression) 
	{
		System.out.println("MinorThan" + " " + ii++);
	}


	public void visit(MinorThanEquals expression) 
	{
		System.out.println("MinorThanEquals" + " " + ii++);
	}


	public void visit(NotEqualsTo expression) 
	{
		System.out.println("NotEqualsTo" + " " + ii++);
	}


	public void visit(Column expression) 
	{
		//System.out.println("Column" + " " + ii++);
		
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
				if (expression.toString().equalsIgnoreCase(schema[i].getColumnName()))
				{
					index = i;
					break;
				}
			}
		}
		columnDataString = (String)tuple[index];
	}


	public void visit(SubSelect expression) 
	{
		System.out.println("SubSelect" + " " + ii++);
	}


	public void visit(CaseExpression expression) 
	{
		System.out.println("CaseExpression" + " " + ii++);
	}


	public void visit(WhenClause expression) 
	{
		System.out.println("WhenClause" + " " + ii++);
	}


	public void visit(ExistsExpression expression) 
	{
		System.out.println("ExistsExpression" + " " + ii++);
	}


	public void visit(AllComparisonExpression expression) 
	{
		System.out.println("AllComparisonExpression" + " " + ii++);
	}


	public void visit(AnyComparisonExpression expression) 
	{
		System.out.println("AnyComparisonExpression" + " " + ii++);
		expression.getSubSelect();
	}


	public void visit(Concat expression) 
	{
		System.out.println("Concat" + " " + ii++);
		expression.getLeftExpression().accept(this);
		expression.getRightExpression().accept(this);
	}


	public void visit(Matches expression) 
	{
		System.out.println("Matches" + " " + ii++);
		expression.getLeftExpression().accept(this);
		expression.getRightExpression().accept(this);
	}


	public void visit(BitwiseAnd expression) 
	{
		System.out.println("BitwiseAnd" + " " + ii++);
		expression.getLeftExpression().accept(this);
		expression.getRightExpression().accept(this);
	}


	public void visit(BitwiseOr expression) 
	{
		System.out.println("BitwiseOr" + " " + ii++);
		expression.getLeftExpression().accept(this);
		expression.getRightExpression().accept(this);
	}


	public void visit(BitwiseXor expression) 
	{
		System.out.println("BitwiseXor" + " " + ii++);
		expression.getLeftExpression().accept(this);
		expression.getRightExpression().accept(this);
	}

	public String getExpressionValue() 
	{
		return columnDataString;
	}

}
