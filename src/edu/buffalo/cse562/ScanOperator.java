package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class ScanOperator implements Operator 
{
	// CLASS VARIABLES
	
	BufferedReader input;
	File file;
	List<String[]> tupleList;
	int index;
	String line;
		
	// CONSTRUCTOR
		
	public ScanOperator(File file) 
	{
		this.file = file;
		reset();
	}

	public ScanOperator(List<String[]> tupleList) 
	{
		this.tupleList = tupleList;
		index = 0;
	}

	@Override
	public String[] readOneTuple() 
	{
		if (file != null)
		{
			if (input == null)
			{
				return null;
			}
			
			line = null;
			
			try 
			{
				line = input.readLine();
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
			
			if (line == null)
			{
				return null;
			}
			
			String[] columns = line.split("\\|");
			return columns;
		}
		else
		{
			if (tupleList.size() > index)
			{
				return tupleList.get(index++);
			}
			return null;
		}
	}
	
	public String line()
	{
		return line;
	}

	@Override
	public void reset() 
	{
		try 
		{
			input = new BufferedReader(new FileReader(file));
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}
	}

}
