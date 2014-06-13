package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class FileSortOperator
{
	// CLASS VARIABLES

	String tableName;
	File source;
	File swapDir;
	
	// CONSTRUCTOR

	public FileSortOperator(String tableName, File source, File swapDir)
	{
		this.tableName = tableName;
		this.source = source;
		this.swapDir = swapDir;
	}

	public void sort()
	{
		List<File> fileList;
		try
		{
			fileList = sortFile(source, 0);
			mergeSortedFiles(fileList, new File(swapDir,tableName + ".dat"), 0);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public List<File> sortFile(File sourceFile, int sortIndex) throws IOException
	{
		List<File> fileList = new ArrayList<File>();
		BufferedReader bReader = new BufferedReader(new FileReader(sourceFile));
		long blockSize = SizeOfBlocks(sourceFile);
		try
		{
			List<String> tmplist =  new ArrayList<String>();
			String line = "";
			try
			{
				while(line != null)
				{
					long thisblockSize = 0;
					while((thisblockSize < blockSize) && ((line = bReader.readLine()) != null))
					{
						tmplist.add(line);
						thisblockSize = thisblockSize + line.length();
					}
					fileList.add(sortAndSave(tmplist, sortIndex,swapDir));
					tmplist.clear();
				}
			}
			catch(Exception e)
			{
				if(tmplist.size()>0)
				{
					fileList.add(sortAndSave(tmplist, sortIndex,swapDir));
					tmplist.clear();
				}
			}
		}
		finally
		{
			bReader.close();
		}
		return fileList;
	}

	public File sortAndSave(List<String> tmplist, int sortIndex, File swapDir) throws IOException
	{
		Collections.sort(tmplist, new SortOperator(sortIndex));

		File newtmpfile = File.createTempFile("sort", "datfile", swapDir);
		newtmpfile.deleteOnExit();
		BufferedWriter fbw = new BufferedWriter(new FileWriter(newtmpfile));
		try
		{
			for(String r : tmplist)
			{
				fbw.write(r);
				fbw.newLine();
			}
		}
		finally
		{
			fbw.close();
		}
		return newtmpfile;
	}

	public long SizeOfBlocks(File file)
	{
		long sizeOfFile = file.length();
		long blockSize = sizeOfFile / 1024;
		long javaHeapMem = (1024*1024*1024); //Configured for 1GB of heap as stated in Checkpoint 2
		if( blockSize < javaHeapMem/100)
		{
			blockSize = javaHeapMem/100;
		}
		return blockSize;
	}

	public static int mergeSortedFiles(List<File> fileList, File file, final int sortIndex) throws IOException
	{
		PriorityQueue<FileBlocks> pq = new PriorityQueue<FileBlocks>
		(
				11,
				new Comparator<FileBlocks>()
				{
					SortOperator comparator = new SortOperator(sortIndex);
					public int compare(FileBlocks i, FileBlocks j)
					{
						return comparator.compare(i.readNext(), j.readNext());
					}
				}
				);
		for (File f : fileList)
		{
			FileBlocks fileBlock = new FileBlocks(f);
			pq.add(fileBlock);
		}
		BufferedWriter fbw = new BufferedWriter(new FileWriter(file));
		int rowcounter = 0;
		try
		{
			while(pq.size()>0)
			{
				FileBlocks fileBlock = pq.poll();
				String r = fileBlock.readNReset();
				fbw.write(r);
				fbw.newLine();
				++rowcounter;
				if(fileBlock.empty())
				{
					fileBlock.source.close();
					fileBlock.splitfile.delete();
				}
				else
				{
					pq.add(fileBlock);
				}
			}
		}
		finally
		{
			fbw.close();
			for(FileBlocks fileBlock : pq ) fileBlock.close();
		}
		return rowcounter;

	}

}

class FileBlocks
{
	public BufferedReader source;
	public File splitfile;
	public String tupleLine;
	public boolean empty;

	public FileBlocks(File splitFile) throws IOException
	{
		splitfile = splitFile;
		source = new BufferedReader(new FileReader(splitFile), 2048);
		reset();
	}

	public boolean empty() {
		return empty;
	}

	public void reset() throws IOException
	{
		try
		{
			if((this.tupleLine = source.readLine()) == null)
			{
				empty = true;
				tupleLine = null;
			}
			else
			{
				empty = false;
			}
		}
		catch(Exception e)
		{
			empty = true;
			tupleLine = null;
		}
	}

	public void close() throws IOException
	{
		source.close();
	}


	public String readNext()
	{
		if(empty())
		{
			return null;
		}
		return tupleLine.toString();
	}
	public String readNReset() throws IOException
	{
		String answer = readNext();
		reset();
		return answer;
	}
}
