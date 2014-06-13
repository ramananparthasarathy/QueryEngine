package edu.buffalo.cse562;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SortOperator implements Comparator<String> 
{
	// CLASS VARIABLES

	final int index;

	// CONSTRUCTOR

	public SortOperator(int index) 
	{
		this.index = index;
	}

	// METHOD

	public int compare(String tupleA, String tupleB)
	{
		String[] tuple1 = tupleA.split("\\|");
		String[] tuple2 = tupleB.split("\\|");

		if (Double.parseDouble(tuple1[index]) > Double.parseDouble(tuple2[index]))
		{
			return 1;
		}
		else if (Double.parseDouble(tuple1[index]) < Double.parseDouble(tuple2[index]))
		{
			return -1;
		}
		else
		{
			return 0;
		}
	}
}
