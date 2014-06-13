package edu.buffalo.cse562;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class GroupByOperator 
{
	// CLASS VARIABLES

	Column[] schema;
	PlainSelect plainSelect;
	List<Integer> groupByList;
	List<String> aggregateColumns;
	List<String> aggregateColumnsAlias;

	List<String> aggregateSchema;
	List<String> aggregateSchemaAlias;
	List<HashMap<String, Double>> hashList = new ArrayList<HashMap<String, Double>>();
	HashMap<String, Boolean> distinct = new HashMap<String, Boolean>();
	List<HashMap<String, Double>> sumList = new ArrayList<HashMap<String, Double>>();
	List<HashMap<String, Double>> countList = new ArrayList<HashMap<String, Double>>();
	

	// CONSTRUCTOR

	public GroupByOperator(Column[] schema, 
						   PlainSelect plainSelect,
						   List<Integer> groupByList,
						   List<String> aggregateColumns,
						   List<String> aggregateColumnsAlias)
	{
		this.schema = schema;
		this.plainSelect = plainSelect;
		this.groupByList = groupByList;
		this.aggregateColumns = aggregateColumns;
		this.aggregateColumnsAlias = aggregateColumnsAlias;
	}

	// METHODS

	public void groupBy(Object[] tuple)
	{
		String groupByKey = "";
		String distinctKey = "";
		
		int avgIndex = 0;
		aggregateSchema = new ArrayList<String>();
		aggregateSchemaAlias = new ArrayList<String>();
		int aggregateIndex = 0;

		// CREATE GROUPBY/AGGREGATE KEY

		if (groupByList.size() == 0)
		{
			groupByKey = "";
		}
		else
		{
			for (aggregateIndex = 0 ; aggregateIndex < groupByList.size() ; aggregateIndex++)
			{
				groupByKey = groupByKey + tuple[groupByList.get(aggregateIndex)] + "|";
				aggregateSchema.add(aggregateIndex, 
						plainSelect.getGroupByColumnReferences().get(aggregateIndex).toString());
				aggregateSchemaAlias.add(aggregateIndex, 
						null);
			}
		}
				
		// AGGREGATOR USING HASHMAP

		for (int i = 0 ; i < aggregateColumns.size() ; i++)
		{

			CCJSqlParser parser = new CCJSqlParser(new StringReader(aggregateColumns.get(i)));
			Function function = null;
			try 
			{
				function = parser.Function();
			} 
			catch (ParseException e1) 
			{
				e1.printStackTrace();
			}

			// CHECK THE FUNCTIONS

			if (function.getName().equalsIgnoreCase("COUNT"))
			{
				AggregateEvaluator eval = new AggregateEvaluator(schema, tuple);
				
				if (! function.isAllColumns())
				{	
					parser = new CCJSqlParser(new StringReader(function.getParameters().toString()));
					Expression expression = null;
					try 
					{
						expression = parser.SimpleExpression();
					} 
					catch (ParseException e) 
					{
						e.printStackTrace();
					}
					expression.accept(eval);
					distinctKey = groupByKey + eval.getExpressionValue() + "|";
				}
				else
				{
					eval = null;
				}
				
				if (hashList.size() > i)
				{
					if (function.isDistinct())
					{
						if (hashList.get(i).containsKey(groupByKey))
						{
							Double count = hashList.get(i).get(groupByKey);
							if (!distinct.containsKey(distinctKey))
							{
								distinct.put(distinctKey, true);
								count++;
							}
							hashList.get(i).put(groupByKey, count);
						}
						else
						{
							hashList.get(i).put(groupByKey, 1.00);
							distinct.put(distinctKey, true);
						}
					}
					else
					{
						if (hashList.get(i).containsKey(groupByKey))
						{
							Double count = hashList.get(i).get(groupByKey);
							count++;
							hashList.get(i).put(groupByKey, count);
						}
						else
						{
							hashList.get(i).put(groupByKey, 1.00);
						}
					}
				}
				else
				{
					HashMap<String,Double> tempMap = new HashMap<String,Double>();
					tempMap.put(groupByKey, 1.00);
					hashList.add(i, tempMap);
					distinct.put(distinctKey, true);
				}

				aggregateSchema.add(aggregateIndex, aggregateColumns.get(i));
				aggregateSchemaAlias.add(aggregateIndex, aggregateColumnsAlias.get(i));
				aggregateIndex++;
			}

			
			else if (function.getName().equalsIgnoreCase("SUM"))
			{
				parser = new CCJSqlParser(new StringReader(function.getParameters().toString()));
				Expression expression = null;
				try 
				{
					expression = parser.SimpleExpression();
				} 
				catch (ParseException e) 
				{
					e.printStackTrace();
				}

				AggregateEvaluator eval = new AggregateEvaluator(schema, tuple);
				expression.accept(eval);

				if (hashList.size() > i)
				{
					if (hashList.get(i).containsKey(groupByKey))
					{
						Double sum = hashList.get(i).get(groupByKey);
						sum = sum + Double.parseDouble(eval.getExpressionValue());
						hashList.get(i).put(groupByKey, sum);
					}
					else
					{
						hashList.get(i).put(groupByKey, Double.parseDouble(eval.getExpressionValue()));
					}
				}
				else
				{
					HashMap<String,Double> tempMap = new HashMap<String,Double>();
					tempMap.put(groupByKey, Double.parseDouble(eval.getExpressionValue()));
					hashList.add(i, tempMap);
				}

				aggregateSchema.add(aggregateIndex, aggregateColumns.get(i));
				aggregateSchemaAlias.add(aggregateIndex, aggregateColumnsAlias.get(i));
				aggregateIndex++;
			}

			else if (function.getName().equalsIgnoreCase("AVG"))
			{
				parser = new CCJSqlParser(new StringReader(function.getParameters().toString()));
				Expression expression = null;
				try 
				{
					expression = parser.SimpleExpression();
				} 
				catch (ParseException e) 
				{
					e.printStackTrace();
				}

				AggregateEvaluator eval = new AggregateEvaluator(schema, tuple);
				expression.accept(eval);

				if (hashList.size() > i)
				{
					
					Double sum = 0.00;
					if (sumList.get(avgIndex).containsKey(groupByKey))
					{
						sum = sumList.get(avgIndex).get(groupByKey);
						sum = sum + Double.parseDouble(eval.getExpressionValue());
						sumList.get(avgIndex).put(groupByKey, sum);
					}
					else
					{
						sumList.get(avgIndex).put(groupByKey, Double.parseDouble(eval.getExpressionValue()));
					}
					
					Double count = 1.00;
					if (countList.get(avgIndex).containsKey(groupByKey))
					{
						count = countList.get(avgIndex).get(groupByKey);
						count++;
						countList.get(avgIndex).put(groupByKey, count);
					}
					else
					{
						countList.get(avgIndex).put(groupByKey, 1.00);
					}
										
					if (hashList.get(i).containsKey(groupByKey))
					{
						Double avg = sum/count;
						hashList.get(i).put(groupByKey, avg);
					}
					else
					{
						hashList.get(i).put(groupByKey, Double.parseDouble(eval.getExpressionValue()));
					}
				}
				else
				{
					HashMap<String,Double> tempMap = new HashMap<String,Double>();
					HashMap<String,Double> tempSumMap = new HashMap<String,Double>();
					HashMap<String,Double> tempCountMap = new HashMap<String,Double>();
					
					tempMap.put(groupByKey, Double.parseDouble(eval.getExpressionValue()));
					tempSumMap.put(groupByKey, Double.parseDouble(eval.getExpressionValue()));
					tempCountMap.put(groupByKey, 1.00);
					hashList.add(i, tempMap);
					sumList.add(avgIndex,tempSumMap);
					countList.add(avgIndex, tempCountMap);
				}

				aggregateSchema.add(aggregateIndex, aggregateColumns.get(i));
				aggregateSchemaAlias.add(aggregateIndex, aggregateColumnsAlias.get(i));
				aggregateIndex++;
				avgIndex++;
			}
			else if (function.getName().equalsIgnoreCase("MAX"))
			{
				parser = new CCJSqlParser(new StringReader(function.getParameters().toString()));
				Expression expression = null;
				try 
				{
					expression = parser.SimpleExpression();
				} 
				catch (ParseException e) 
				{
					e.printStackTrace();
				}

				AggregateEvaluator eval = new AggregateEvaluator(schema, tuple);
				expression.accept(eval);

				if (hashList.size() > i)
				{
					if (hashList.get(i).containsKey(groupByKey))
					{
						Double max = hashList.get(i).get(groupByKey);
						if (Double.parseDouble(eval.getExpressionValue()) > max)
						{
							max = Double.parseDouble(eval.getExpressionValue());
						}
						hashList.get(i).put(groupByKey, max);
					}
					else
					{
						hashList.get(i).put(groupByKey , Double.parseDouble(eval.getExpressionValue()));
					}
				}
				else
				{
					HashMap<String,Double> tempMap = new HashMap<String,Double>();
					tempMap.put(groupByKey, Double.parseDouble(eval.getExpressionValue()));
					hashList.add(i, tempMap);
				}

				aggregateSchema.add(aggregateIndex, aggregateColumns.get(i));
				aggregateSchemaAlias.add(aggregateIndex, aggregateColumnsAlias.get(i));
				aggregateIndex++;
			}
			else if (function.getName().equalsIgnoreCase("MIN"))
			{
				parser = new CCJSqlParser(new StringReader(function.getParameters().toString()));
				Expression expression = null;
				try 
				{
					expression = parser.SimpleExpression();
				} 
				catch (ParseException e) 
				{
					e.printStackTrace();
				}

				AggregateEvaluator eval = new AggregateEvaluator(schema, tuple);
				expression.accept(eval);

				if (hashList.size() > i)
				{
					if (hashList.get(i).containsKey(groupByKey))
					{
						Double min = hashList.get(i).get(groupByKey);
						if (Double.parseDouble(eval.getExpressionValue()) < min)
						{
							min = Double.parseDouble(eval.getExpressionValue());
						}
						hashList.get(i).put(groupByKey, min);
					}
					else
					{
						hashList.get(i).put(groupByKey, Double.parseDouble(eval.getExpressionValue()));
					}
				}
				else
				{
					HashMap<String,Double> tempMap = new HashMap<String,Double>();
					tempMap.put(groupByKey, Double.parseDouble(eval.getExpressionValue()));
					hashList.add(i, tempMap);
				}

				aggregateSchema.add(aggregateIndex, aggregateColumns.get(i));
				aggregateSchemaAlias.add(aggregateIndex, aggregateColumnsAlias.get(i));
				aggregateIndex++;
			}
		}
	}

	// GETTER METHODS

	public List<String> getAggregateSchema()
	{
		return aggregateSchema;
	}


	public List<String> getAggregateSchemaAlias()
	{
		return aggregateSchemaAlias;
	}

	public List<String[]> getHashList()
	{
		List<String[]> printList = new ArrayList<String[]>();

		String groupByTuple = "";
		if (! hashList.isEmpty())
		{
			Set<Entry<String, Double>> hashSet = hashList.get(0).entrySet();
			Iterator<Entry<String, Double>> itr = hashSet.iterator();
			while (itr.hasNext())
			{
				Map.Entry mapEntry = (Map.Entry) itr.next();
				for (int i = 0 ; i < hashList.size() ; i++)
				{
					groupByTuple = groupByTuple + hashList.get(i).get(mapEntry.getKey()) + "|";
				}
				groupByTuple = mapEntry.getKey().toString() + groupByTuple;
				String[] tempTuple = groupByTuple.split("\\|");
				printList.add(tempTuple);
				groupByTuple = "";
			}
		}
		return printList;
	}
}
