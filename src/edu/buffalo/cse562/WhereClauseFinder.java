package edu.buffalo.cse562;



import java.io.StringReader;
import java.util.HashMap;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
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
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;

public class WhereClauseFinder implements ExpressionVisitor{

	PlainSelect ps;
	public static int counter = 1;

	public static boolean leftColumnFlag = false;
	public static boolean rightColumnFlag = false;
	public static boolean isLeftColumnFlag = false;
	public static boolean isRightColumnFlag = false;
	public static boolean left = false;
	public static boolean right = false;
	public  String whereClause = "";
	public static String clause;
	public String table;

	public WhereClauseFinder(PlainSelect ps, String table)
	{
		this.ps = ps;
		this.table = table;
	}
	public Expression findWhere()
	{
		Expression e = ps.getWhere();
		Expression where = null;
		if(e != null)
		{
			e.accept(this);
		}
		else
		{
			return null;
		}
		if(!whereClause.equals(""))
		{
			CCJSqlParserManager pm = new CCJSqlParserManager();
			String sql = "SELECT * FROM A WHERE " + whereClause.substring(0, whereClause.length()-5);
			try {
				Statement statement = pm.parse(new StringReader(sql));
				Select selectStatement = (Select)statement;
				PlainSelect ps = (PlainSelect)selectStatement.getSelectBody();
				where = ps.getWhere();

			} catch (JSQLParserException j) {
				// TODO Auto-generated catch block
				j.printStackTrace();
			}
		}
		else
		{
			where = null;
		}
		return where;	
	}



	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Parenthesis par) {
		// TODO Auto-generated method stub

		if(par.toString().contains(table))
		{
			whereClause = whereClause + par.toString() + clause;
		}
	}
	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(AndExpression and) {
		// TODO Auto-generated method stub
		clause = " AND ";
		and.getLeftExpression().accept(this);
		and.getRightExpression().accept(this);

	}
	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(EqualsTo equalsTo) {
		// TODO Auto-generated method stub
		leftColumnFlag = false;
		rightColumnFlag = false;
		left = true;
		equalsTo.getLeftExpression().accept(this);
		left = false;
		right = true;
		equalsTo.getRightExpression().accept(this);
		right = false;
		if((leftColumnFlag == true && rightColumnFlag == true) || (leftColumnFlag ==true && isRightColumnFlag == false) || (rightColumnFlag == true && isLeftColumnFlag == false))
		{
			whereClause = whereClause + equalsTo.toString() + clause;
		}
		isLeftColumnFlag = false;
		isRightColumnFlag = false;
	}
	@Override
	public void visit(GreaterThan gt) {
		// TODO Auto-generated method stub
		leftColumnFlag = false;
		rightColumnFlag = false;
		left = true;
		gt.getLeftExpression().accept(this);
		left =false;
		right =true;
		gt.getRightExpression().accept(this);
		right = false;
		if((leftColumnFlag == true && rightColumnFlag == true) || (leftColumnFlag ==true && isRightColumnFlag == false) || (rightColumnFlag == true && isLeftColumnFlag == false))
		{
			whereClause = whereClause + gt.toString()  + clause;
		}
		isLeftColumnFlag = false;
		isRightColumnFlag = false;
	}
	@Override
	public void visit(GreaterThanEquals gte) {
		// TODO Auto-generated method stub
		leftColumnFlag = false;
		rightColumnFlag = false;
		left = true;
		gte.getLeftExpression().accept(this);
		left =false;
		right =true;
		gte.getRightExpression().accept(this);
		right =false;
		if((leftColumnFlag == true && rightColumnFlag == true) || (leftColumnFlag ==true && isRightColumnFlag == false) || (rightColumnFlag == true && isLeftColumnFlag == false))
		{
			whereClause = whereClause + gte.toString() + clause;
		}
		isLeftColumnFlag = false;
		isRightColumnFlag = false;
	}
	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(MinorThan mt) {
		// TODO Auto-generated method stub
		leftColumnFlag = false;
		rightColumnFlag = false;
		left = true;
		mt.getLeftExpression().accept(this);
		left = false;
		right = true;
		mt.getRightExpression().accept(this);
		right = false;
		if((leftColumnFlag == true && rightColumnFlag == true) || (leftColumnFlag ==true && isRightColumnFlag == false) || (rightColumnFlag == true && isLeftColumnFlag == false))
		{
			whereClause = whereClause + mt.toString() + clause;
		}
		isLeftColumnFlag = false;
		isRightColumnFlag = false;
	}
	@Override
	public void visit(MinorThanEquals mte) {
		// TODO Auto-generated method stub
		leftColumnFlag = false;
		rightColumnFlag = false;
		left = true;
		mte.getLeftExpression().accept(this);
		left = false;
		right = true;
		mte.getRightExpression().accept(this);
		right = false;
		if((leftColumnFlag == true && rightColumnFlag == true) || (leftColumnFlag ==true && isRightColumnFlag == false) || (rightColumnFlag == true && isLeftColumnFlag == false))
		{
			whereClause = whereClause + mte.toString() + clause;
		}
		isLeftColumnFlag = false;
		isRightColumnFlag = false;
	}
	@Override
	public void visit(NotEqualsTo ne) {
		// TODO Auto-generated method stub
		leftColumnFlag = false;
		rightColumnFlag = false;
		left = true;
		ne.getLeftExpression().accept(this);
		left = false;
		right = true;
		ne.getRightExpression().accept(this);
		right = false;
		if((leftColumnFlag == true && rightColumnFlag == true) || (leftColumnFlag ==true && isRightColumnFlag == false) || (rightColumnFlag == true && isLeftColumnFlag == false))
		{
			whereClause = whereClause + ne.toString() + clause;
		}
		isLeftColumnFlag = false;
		isRightColumnFlag = false;
	}
	@Override
	public void visit(Column column) {
		// TODO Auto-generated method stub

		if(left == true)
		{
			isLeftColumnFlag = true;
		}
		if(right == true)
		{
			isRightColumnFlag = true;
		}
		if(left == true && column.getTable().getName().equals(table))
		{
			leftColumnFlag = true;
		}
		if(right == true && column.getTable().getName().equals(table))
		{
			rightColumnFlag = true;
		}
	}
	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub

	}
}
