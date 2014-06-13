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

public class SubSelectOperator implements SelectVisitor
{
	// CLASS VARIABLES

	public File dataDir;
	public File swapDir;
	public File indexDir;
	public HashMap<String, CreateTable> tables;
	List<Integer> equalityIndexes ;
	public static HashMap<String,Expression> whereClauses = new HashMap<String,Expression>();
	List<String> schemaList;
	List<String> schemaAliasList;
	List<String[]> tupleList;


	// CONSTRUCTOR FOR CLASS

	public SubSelectOperator(File dataDir, File swapDir, File indexDir, HashMap<String, CreateTable> tables) 
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

			scanner[0] = new FromScanner(dataDir,swapDir, indexDir, tables);
			plainSelect.getFromItem().accept(scanner[0]);

			tableNames.add(plainSelect.getFromItem().toString());
			for (int i = 0 ; i < joinSize ; i++)
			{
				Join join = new Join();
				join = (Join) joins.get(i);
				tableNames.add(join.getRightItem().toString());
				scanner[i+1] = new FromScanner(dataDir,swapDir, indexDir, tables);
				join.getRightItem().accept(scanner[i+1]);
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


			for(int x=0;x<scanner.length;x++)
			{
				WhereClauseFinder wcf = new WhereClauseFinder(plainSelect,scanner[x].schema[0].getTable().getName());
				whereClauses.put(scanner[x].schema[0].getTable().getName(), wcf.findWhere());
			}

			Expression whereClause = plainSelect.getWhere();

			int numberOfTables =  scanner.length;
			
			QueryTypeOperator typeOperator = new QueryTypeOperator(schema,
					plainSelect,
					false,
					null,
					null);

			List<Object[]> tupleList1 = new ArrayList<Object[]>();

			int counter = 0;

			RecordManager rec = RecordManagerFactory.createRecordManager((new File(indexDir, "index.db")).toString());

			if (joinSize > 0)
			{
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
						PrimaryTreeMap<String,List<String[]>> primaryMap = rec.treeMap(joinOp[i].previousTableName + "_" + joinOp[i].previousTableIndex);
						PrimaryTreeMap<String,List<String[]>> primaryMap2 = rec.treeMap(joinOp[i].newTableName + "_" + joinOp[i].newTableIndexValue);

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

						List<Object[]> tupleList2 = new ArrayList<Object[]>(tupleList1);
						tupleList1.clear();

						String table_Name = joinOp[i].newTableName;

						if (joinOp[i].newTableName.equals("n1") || joinOp[i].newTableName.equals("n2"))
						{
							table_Name = "nation";
						}

						PrimaryTreeMap<String,List<String[]>> primaryMap2 = rec.treeMap(table_Name + "_" + joinOp[i].newTableIndexValue);

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
				rec.close();
			}
			else
			{
				ScanOperator singleTableOp = new ScanOperator(scanner[0].source);
				RowSelectionOperator singleRS = new RowSelectionOperator(singleTableOp, scanner[0].schema, whereClause , scanner[0].columnMap,scanner[0].tableMap,scanner[0].aliasMap,scanner[0].coldataType);

				String[] SingleTuple = null;
				while((SingleTuple = singleRS.readOneTuple()) != null)
				{
					typeOperator.projectSimple(SingleTuple);
				}
			}
			
			schemaList = typeOperator.getTypeSchema();
			tupleList = typeOperator.getPrintList();
			schemaAliasList = typeOperator.getTypeSchemaAlias();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	//

	public List<String[]> getSubFile()
	{
		return tupleList;
	}

	public List<String> getSchemaList()
	{
		return schemaList;
	}

	public List<String> getSchemaAliasList()
	{
		return schemaAliasList;
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
