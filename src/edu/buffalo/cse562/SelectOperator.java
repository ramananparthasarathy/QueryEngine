package edu.buffalo.cse562;

import java.io.*;
import java.util.*;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

public class SelectOperator implements SelectVisitor
{
	// CLASS VARIABLES

	public File dataDir;
	public File swapDir;
	public File indexDir;
	public HashMap<String, CreateTable> tables;
	List<Integer> equalityIndexes ;
	public static HashMap<String,Expression> whereClauses = new HashMap<String,Expression>();
	
	// CONSTRUCTOR FOR CLASS

	public SelectOperator(File dataDir, File swapDir, File indexDir, HashMap<String, CreateTable> tables) 
	{
		this.dataDir = dataDir;
		this.swapDir = swapDir;
		this.indexDir = indexDir;
		this.tables = tables;
	}

	// STARTING THE SELECT STATEMENT EVALUATION PROCESS

	public void initializeSelect(Select selectStatement) 
	{
		selectStatement.getSelectBody().accept(this);				
	}

	// VISITING IN CASE OF PLAINSELECT

	public void visit(PlainSelect plainSelect) 
	{
		try
		{
			List joins = plainSelect.getJoins();
			int joinSize;

			if (joins != null)
				joinSize = joins.size();
			else
				joinSize = 0;

			FromScanner[] scanner =  new FromScanner[joinSize+1];

			List<String> tableNames = new ArrayList<String>();

			tableNames.add(plainSelect.getFromItem().toString());
			for (int i = 0 ; i < joinSize ; i++)
			{
				Join join = new Join();
				join = (Join) joins.get(i);
				tableNames.add(join.getRightItem().toString());
			}

			File[] fileArray = new File[joinSize+1];

			for(int i =0; i < fileArray.length; i++){
				fileArray[i] = new File(dataDir,tableNames.get(i)+".dat");
			}


			File temp = null;
			for(int i = 0; i < fileArray.length; i++)
			{
				for (int j = 0; j < fileArray.length - i - 1; j++) 
				{
					if (fileArray[j].length() > fileArray[j+1].length()) 
					{
						temp = fileArray[j];
						fileArray[j]   = fileArray[j+1];
						fileArray[j+1] = temp;
					}
				}
			}

			for (int i = 0 ; i < fileArray.length ; i++)
			{
				if(fileArray[i].getName().substring(0,(fileArray[i].getName().length())-4).equalsIgnoreCase(plainSelect.getFromItem().toString())){
					scanner[i] = new FromScanner(dataDir,swapDir, indexDir,  tables);
					plainSelect.getFromItem().accept(scanner[i]);
				}
				else
				{
					for(int j = 0; j < joinSize; j++)
					{
						Join join = new Join();
						join = (Join) joins.get(j);
						if(fileArray[i].getName().substring(0,(fileArray[i].getName().length())-4).equalsIgnoreCase(join.getRightItem().toString()))
						{
							scanner[i] = new FromScanner(dataDir,swapDir, indexDir, tables);
							join.getRightItem().accept(scanner[i]);
							break;
						}
					}
				}
			}

			Column[] schema = scanner[0].schema;
			Column[] dataType = scanner[0].coldataType;
			Column[][] allDataType = new Column[joinSize][];
			Column[][] joinSchema = new Column[joinSize][];

			if (joins != null)
			{
				for (int i = 1 ; i < joinSize+1 ; i++)
				{
					Column[] tempSchema = new Column[schema.length + scanner[i].schema.length];
					Column[] tempDataType = new Column[schema.length + scanner[i].schema.length];
					int j;
					for (j = 0 ; j < schema.length ; j++)
					{
						tempSchema[j] = schema[j];
						tempDataType[j] = dataType[j];
					}
					int x = 0;
					for (int k = j ; k < schema.length + scanner[i].schema.length ; k++)
					{
						tempSchema[k] = scanner[i].schema[x];
						tempDataType[k] = scanner[i].coldataType[x];
						x++;
					}
					joinSchema[i-1] = tempSchema;
					allDataType[i-1] = tempDataType;
					schema = tempSchema;
					dataType = tempDataType;

				}
			}


			for(int x=0;x<scanner.length && joinSize > 0;x++)
			{
				WhereClauseFinder wcf = new WhereClauseFinder(plainSelect,scanner[x].schema[0].getTable().getName());
				whereClauses.put(scanner[x].schema[0].getTable().getName(), wcf.findWhere());
			}

			Expression whereClause = plainSelect.getWhere();
			int numberOfTables =  scanner.length;
	
			// CHECK FOR AGGREGATE

			List selectList = plainSelect.getSelectItems();
			List<String> aggregateColumns = new ArrayList<String>();
			List<String> aggregateColumnsAlias = new ArrayList<String>();
			int x = 0;
			boolean containsAggregate = false;

			for (int i = 0 ; i < selectList.size() ; i++)
			{
				if (selectList.get(i).toString().toLowerCase().contains("count(")
						|| selectList.get(i).toString().toLowerCase().contains("sum(")
						|| selectList.get(i).toString().toLowerCase().contains("avg(")
						|| selectList.get(i).toString().toLowerCase().contains("max(")
						|| selectList.get(i).toString().toLowerCase().contains("min("))
				{
					containsAggregate = true;
					if (selectList.get(i).toString().contains(" AS "))
					{
						aggregateColumns.add(x, selectList.get(i).toString().split("\\ AS ")[0]);
						aggregateColumnsAlias.add(x, selectList.get(i).toString().split("\\ AS ")[1]);
						x++;
					}
					else if (selectList.get(i).toString().contains(" as "))
					{
						aggregateColumns.add(x, selectList.get(i).toString().split("\\ as ")[0]);
						aggregateColumnsAlias.add(x, selectList.get(i).toString().split("\\ as ")[1]);
						x++;
					}
					else
					{
						aggregateColumns.add(x, selectList.get(i).toString());
						aggregateColumnsAlias.add(x, null);
						x++;
					}
				}
			}

			// GET THE DATA FOR GROUPBY LIST
			List groupBy = plainSelect.getGroupByColumnReferences();
			List<Integer> groupByList = new ArrayList<Integer>();

			if (groupBy != null)
			{
				for (int i = 0 ; i < groupBy.size() ; i++)
				{
					for (int j = 0 ; j < schema.length ; j++)
					{
						if (groupBy.get(i).toString().contains("."))
						{
							if (groupBy.get(i).toString().equalsIgnoreCase(schema[j].getTable().getAlias()+"."+schema[j].getColumnName()) 
									|| groupBy.get(i).toString().equalsIgnoreCase(schema[j].getTable().getName()+"."+schema[j].getColumnName()))
							{
								groupByList.add(i, j);
								break;
							}
						}
						else
						{
							if (groupBy.get(i).toString().equalsIgnoreCase(schema[j].getColumnName()))
							{
								groupByList.add(i, j);
								break;
							}
						}
					}
				}
			}
				

			GroupByOperator groupByOperator = new GroupByOperator(schema, 
					plainSelect,
					groupByList,
					aggregateColumns,
					aggregateColumnsAlias);

			QueryTypeOperator typeOperator = new QueryTypeOperator(schema,
					plainSelect,
					containsAggregate,
					groupByList,
					groupByOperator);

			List<String[]> tupleList1 = new ArrayList<String[]>();

			int counter = 0;

			

			if (joinSize > 0)
			{
				RecordManager rec = RecordManagerFactory.createRecordManager((new File(indexDir, "index.db")).toString());
				
				JoinEqualityAssociator jea = new JoinEqualityAssociator(joinSchema[numberOfTables-2]);
				whereClause.accept(jea);
				List<String> joinConditions = jea.getIndexList();
				
				TableIndexToJoin[] joinOp = new TableIndexToJoin[joinSize];
				int joinCount = 1;

				for(int i=0; i<joinSize; i++)
				{
					joinOp[i] = new TableIndexToJoin(scanner,
							joinConditions,
							joinSchema,
							schema,
							joinSize,
							numberOfTables,
							joinCount);

					joinOp[i].checkIndexToSort();

					if (counter == 0)
					{
						counter++;
											
						PrimaryTreeMap<String,List<String[]>> primaryMap = rec.treeMap(joinOp[i].previousTableName.toLowerCase() + "_" + joinOp[i].previousTableIndex);
						PrimaryTreeMap<String,List<String[]>> primaryMap2 = rec.treeMap(joinOp[i].newTableName.toLowerCase() + "_" + joinOp[i].newTableIndexValue);

						Evaluator eval1 = new Evaluator(scanner[0].schema, scanner[0].columnMap, scanner[0].coldataType);
						Evaluator eval2 = new Evaluator(scanner[1].schema, scanner[1].columnMap, scanner[1].coldataType);

						for (List<String[]> keyList : primaryMap.values())
						{
							for (int j1 = 0 ; j1 < keyList.size() ; j1++)
							{
								Object[] key = keyList.get(j1);
								int count = 0;
								if (primaryMap2.containsKey(key[joinOp[i].previousTableIndex]))
								{
									if (whereClauses.get(joinOp[i].previousTableName) != null && count == 0)
									{
										count++;
										eval1.sendTuple(key);
										whereClauses.get(joinOp[i].previousTableName).accept(eval1);
									}

									if (eval1.getBool())
									{
										List<String[]> temp2 = primaryMap2.get(key[joinOp[i].previousTableIndex]);

										for (int j = 0 ; j < temp2.size(); j++)
										{
											Object [] st2 = temp2.get(j);

											if (whereClauses.get(joinOp[i].newTableName) != null)
											{
												eval2.sendTuple(st2);
												whereClauses.get(joinOp[i].newTableName).accept(eval2);
											}

											if (eval2.getBool())
											{
												if (scanner.length == 2)
												{
													typeOperator.projectSimple(joinTuple(key, st2));
												}
												else
												{
													tupleList1.add(joinTuple(key, st2));
												}
											}
										}
									}

								}
							}
						}
					}
					else
					{
						counter++;

						List<String[]> tupleList2 = new ArrayList<String[]>(tupleList1);
						tupleList1.clear();

						PrimaryTreeMap<String,List<String[]>> primaryMap2 = rec.treeMap(joinOp[i].newTableName + "_" + joinOp[i].newTableIndexValue);

						Evaluator eval2 = new Evaluator(scanner[counter].schema, scanner[counter].columnMap, scanner[counter].coldataType);

						for (Object[] key : tupleList2)
						{
							if (primaryMap2.containsKey(key[joinOp[i].previousTableIndex]))
							{
								List<String[]> temp2 = primaryMap2.get(key[joinOp[i].previousTableIndex]);

								for (int j = 0 ; j < temp2.size(); j++)
								{
									Object [] st2 = temp2.get(j);

									if (whereClauses.get(joinOp[i].newTableName) != null)
									{
										eval2.sendTuple(st2);
										whereClauses.get(joinOp[i].newTableName).accept(eval2);
									}

									if (eval2.getBool())
									{
										if (counter == scanner.length-1)
										{
											typeOperator.projectSimple(joinTuple(key, st2));
										}
										else
										{
											tupleList1.add(joinTuple(key, st2));
										}
									}
								}
							}
						}
					}
					joinCount++;
				}
			}
			else
			{
				if (scanner[0].flag)
				{
					for (int i = 0 ; i < scanner[0].subList.size() ; i++)
					{
						typeOperator.projectSimple(scanner[0].subList.get(i));
					}
				}
				else
				{
					RecordManager rec = RecordManagerFactory.createRecordManager((new File(indexDir, "index.db")).toString());
					PrimaryTreeMap<String,List<String[]>> primaryMap = rec.treeMap(plainSelect.getFromItem().toString().toLowerCase() + "_0");
					Evaluator eval = new Evaluator(scanner[0].schema, scanner[0].columnMap, scanner[0].coldataType);
										
					for (List<String[]> keyList : primaryMap.values())
					{
						for (int j1 = 0 ; j1 < keyList.size() ; j1++)
						{
							Object[] key = keyList.get(j1);
							
							if (whereClause != null)
							{
								eval.sendTuple(key);
								whereClause.accept(eval);
								
								if (eval.getBool())
								{
									typeOperator.projectSimple(key);
								}
							}
							else
							{
								typeOperator.projectSimple(key);
							}
						}
					}
					rec.close();
				}
			}

			// GROUPBY AND AGGREGATE FOR THE QUERY

			List<String> schemaList = null;
			List<String[]> tupleList = null;
			List<String> schemaAliasList = null;

			if (containsAggregate)
			{
				schemaList = groupByOperator.getAggregateSchema();
				tupleList = groupByOperator.getHashList();
				schemaAliasList = groupByOperator.getAggregateSchemaAlias();
			}
			else
			{
				schemaList = typeOperator.getTypeSchema();
				tupleList = typeOperator.getPrintList();
				schemaAliasList = typeOperator.getTypeSchemaAlias();
			}

			if (tupleList != null && schemaList != null && schemaAliasList != null)
			{
				// ORDER BY

				List orderByList = plainSelect.getOrderByElements();
				String sortingType = "";

				if (orderByList != null)
				{
					for (int i = orderByList.size()-1 ; i >= 0 ; i--)
					{
						String orderByColumn = "";
						if (orderByList.get(i).toString().contains(" "))
						{
							orderByColumn = orderByList.get(i).toString().split("\\ ")[0];
							sortingType = orderByList.get(i).toString().split("\\ ")[1];
						}
						else
						{
							orderByColumn = orderByList.get(i).toString();
							sortingType = "ASC";
						}

						int sortingColumnIndex = 900;

						for (int j = 0 ; j < schemaList.size() ; j++)
						{
							if (orderByColumn.equals(schemaList.get(j)) || orderByColumn.equals(schemaAliasList.get(j)))
							{
								sortingColumnIndex = j;
								break;
							}
						}
						Collections.sort(tupleList, new OrderByOperator(sortingColumnIndex, sortingType));		
					}

				}
				// CHECK LIMIT

				Long limit;
				boolean isLimit;
				try
				{
					limit = plainSelect.getLimit().getRowCount();
					isLimit = true;
				}
				catch (Exception NullPointerException)
				{
					limit = 100000L;
					isLimit = false;
				}

				
				// MAKE THE FINAL PROJECTION LIST

				List<Integer> finalschemaList = new ArrayList<Integer>();

				for (int i = 0 ; i < plainSelect.getSelectItems().size() ; i++)
				{
					String selectColumn = plainSelect.getSelectItems().get(i).toString();
					if (selectColumn.contains(" AS "))
					{
						selectColumn = selectColumn.split("\\ AS ")[0];
					}
					else if (selectColumn.contains(" as "))
					{
						selectColumn = selectColumn.split("\\ as ")[0];
					}
					
					for (int j = 0 ; j < schemaList.size() ; j++)
					{
						if (selectColumn.equals(schemaList.get(j)))
						{
							finalschemaList.add(i,j);
							break;
						}
					}
				}

				// FINAL PRINT OUT

				if (!tupleList.isEmpty())
				{
					for (int i = 0 ; i < tupleList.size() && i!=limit; i++)
					{
						int j;
						for (j = 0 ; j < finalschemaList.size()-1 ; j++)
						{
							if (schemaList.get(finalschemaList.get(j)).toLowerCase().startsWith("count"))
							{
								System.out.print(tupleList.get(i)[finalschemaList.get(j)].split("\\.")[0] + "|");
								continue;
							}
							System.out.print(tupleList.get(i)[finalschemaList.get(j)] + "|");
						}
						if (schemaList.get(finalschemaList.get(j)).toLowerCase().startsWith("count"))
							System.out.print(tupleList.get(i)[finalschemaList.get(j)].split("\\.")[0]);
						else
							System.out.print(tupleList.get(i)[finalschemaList.get(j)]);

						System.out.println();
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	// VISITING IN CASE OF UNION

	public void visit(Union unionClause) 
	{
		Iterator iter = unionClause.getPlainSelects().iterator();
		while (iter.hasNext())
		{
			PlainSelect plainSelect = (PlainSelect) iter.next();
			visit(plainSelect);
		}
	}

	public String[] joinTuple(Object[] key, Object[] st2)
	{
		String [] tuple = new String[key.length + st2.length];
		int i = 0;
		for (Object s : key)
		{
			tuple[i] = (String) s;
			i++;
		}
		for (Object s : st2)
		{
			tuple[i] = (String) s;
			i++;
		}

		return tuple;
	}


}
