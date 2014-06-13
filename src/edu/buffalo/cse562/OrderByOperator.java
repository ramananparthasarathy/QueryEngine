package edu.buffalo.cse562;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class OrderByOperator implements Comparator<String[]> 
{
	// CLASS VARIABLES
	
	int index;
	String sortingType;
	
	// CONSTRUCTOR

	public OrderByOperator(int index, String sortingType) 
	{
		this.index = index;
		this.sortingType = sortingType;
	}
	
	// METHOD

	@Override
	public int compare(String[] tuple1, String[] tuple2)
	{
		if (sortingType.equals("ASC"))
		{
			try
			{
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
			catch (NumberFormatException e)
			{
				try
				{
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
					Date date1 = dateFormat.parse(tuple1[index]);
					Date date2 = dateFormat.parse(tuple2[index]);
					
					return date1.compareTo(date2);
				}
				catch (ParseException e1)
				{
					return (tuple1[index].compareTo(tuple2[index]));					
				}
			}
		}
		else
		{	
			try
			{
				if (Double.parseDouble(tuple1[index]) < Double.parseDouble(tuple2[index]))
				{
					return 1;
				}
				else if (Double.parseDouble(tuple1[index]) > Double.parseDouble(tuple2[index]))
				{
					return -1;
				}
				else
				{
					return 0;
				}
			}
			catch (NumberFormatException e)
			{
				try
				{
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
					Date date1 = dateFormat.parse(tuple1[index]);
					Date date2 = dateFormat.parse(tuple2[index]);
					
					return date2.compareTo(date1);
				}
				catch (ParseException e1)
				{
					return (tuple2[index].compareTo(tuple1[index]));					
				}
			}
		}
	}
}
			
					
			
			
			