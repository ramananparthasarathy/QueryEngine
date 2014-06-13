package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;


public class TableIndexToJoin
{
	FromScanner[] scanner;
	List<String> joinConditions;
	Column[][] joinSchema;
	Column[] schema;
	int joinSize;
	int numberOfTables;
	int joinCount;
	String previousTableName;
	Integer previousTableIndex;
	String currentTable;
	String currentTableAlias;
	Integer newTableIndexValue;
	String newTableName;


	public TableIndexToJoin(FromScanner[] scanner,
			List<String> joinConditions,
			Column[][] joinSchema,
			Column[] schema,
			int joinSize,
			int numberOfTables,
			int joinCount)
	{
		this.scanner = scanner;
		this.joinConditions = joinConditions;
		this.joinSchema = joinSchema;
		this.schema = schema;
		this.joinSize = joinSize;
		this.numberOfTables = numberOfTables;
		this.joinCount = joinCount;
	}

	public void checkIndexToSort()
	{
		Integer[] previousTableLength = new Integer[joinCount];
		String[] previousTable = new String[joinCount];
		String[] previousTableAlias = new String[joinCount];

		currentTable = scanner[joinCount].schema[0].getTable().getName();
		currentTableAlias = scanner[joinCount].schema[0].getTable().getAlias();

		for(int z=0; z<joinCount; z++)
		{
			previousTable[z] = scanner[z].schema[0].getTable().getName();
			previousTableAlias[z] = scanner[z].schema[0].getTable().getAlias();

			if(z!=0)
			{
				previousTableLength[z] = previousTableLength[z-1] + scanner[z].schema.length;
			}
			else
			{
				previousTableLength[z] =  scanner[z].schema.length;
			}
		}
		
		for(int l=0; l<previousTable.length;l++)
		{
			for(int k=0; k<joinConditions.size();k++)
			{
				String join = joinConditions.get(k);

				String[] equalitySplit = join.split("=");
				String[] left;
				String[] right;

				left = 	equalitySplit[0].split("\\.");
				right = equalitySplit[1].split("\\.");

				int primaryTableIsLeft = 0;

				if((left[0].equalsIgnoreCase(previousTable[l]) && right[0].equalsIgnoreCase(currentTable)) || 
						(left[0].equalsIgnoreCase(previousTableAlias[l]) && right[0].equalsIgnoreCase(currentTableAlias)) ||
						(left[0].equalsIgnoreCase(previousTable[l]) && right[0].equalsIgnoreCase(currentTableAlias)) ||
						(left[0].equalsIgnoreCase(previousTableAlias[l]) && right[0].equalsIgnoreCase(currentTable)) )
				{
					primaryTableIsLeft = 1;
				}
				else if((left[0].equalsIgnoreCase(currentTable) && right[0].equalsIgnoreCase(previousTable[l])) || 
						(left[0].equalsIgnoreCase(currentTableAlias) && right[0].equalsIgnoreCase(previousTableAlias[l])) ||
						(left[0].equalsIgnoreCase(currentTableAlias) && right[0].equalsIgnoreCase(previousTable[l])) ||
						(left[0].equalsIgnoreCase(currentTable) && right[0].equalsIgnoreCase(previousTableAlias[l])))
				{
					primaryTableIsLeft =-1;
				}

				if(primaryTableIsLeft==1)
				{
					newTableName = right[0];
					if(l==(joinCount-1))
					{
						previousTableIndex = Integer.parseInt(left[1]);
						previousTableName = previousTable[l];
						newTableIndexValue = Integer.parseInt(right[1])-previousTableLength[l];
					}
					else if(l<(joinCount-1))
					{
						previousTableIndex = Integer.parseInt(left[1]);
						previousTableName = previousTable[l];
						newTableIndexValue = Integer.parseInt(right[1])-previousTableLength[joinCount-1];
					}
				}
				else if(primaryTableIsLeft==-1)
				{
					newTableName = left[0];
					if(l==(joinCount-1))
					{
						previousTableIndex = Integer.parseInt(right[1]);
						previousTableName = previousTable[l];
						newTableIndexValue = Integer.parseInt(left[1])-previousTableLength[l];
					}
					else if(l<(joinCount-1))
					{
						previousTableIndex = Integer.parseInt(right[1]);
						previousTableName = previousTable[l];
						newTableIndexValue = Integer.parseInt(left[1])-previousTableLength[joinCount-1];
					}
				}
			}
		}
	}
}
