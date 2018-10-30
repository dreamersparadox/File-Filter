import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public class LatestFileModified
{
	
	static ArrayList<File> fList = new ArrayList<File>();
	static ArrayList<String> Dates = new ArrayList<String>();
	static ArrayList<String> uDates = new ArrayList<String>();
	static ArrayList<String> tDates = new ArrayList<String>();
	static ArrayList<File> latestFiles = new ArrayList<File>();
		
	static void fileList(String dirPath) //get list of all files in the directory
	{
	    File dir = new File(dirPath);
	    File[] files = dir.listFiles();
	    if (files == null || files.length == 0) 
	    {
	        return;
	    }
	    File FileName = files[0];
	    for (int i = 0; i < files.length; i++) 
	    { 
	        FileName = files[i];
	        fList.add(FileName);
	    }
	}
	
	static void Dates() //get dates of all files
	{
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-YYYY");
		for (int i = 0; i<fList.size(); i++)
			Dates.add(sdf.format(((File) fList.get(i)).lastModified()));
	}
	
	static void uniqueDates() //get the unique dates
	{
		Set<String> list = new HashSet<String>(Dates);
		Enumeration<String> e = Collections.enumeration(list);
        while(e.hasMoreElements())
        	uDates.add(e.nextElement());
	}
	
	static void latestFiles() //get the latest files
	{
		
		ArrayList [] files  = new ArrayList[uDates.size()];  //split the files according to dates
    	
    	SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-YYYY"); 
    	for(int i = 0; i<uDates.size(); i++)
		{
    		ArrayList<File> temp = new ArrayList<File>();
    		for (int j = 0; j<fList.size(); j++) 
    		{
    			if(uDates.get(i).equals(sdf.format(((File) fList.get(j)).lastModified())))
    				temp.add(fList.get(j));
    		}
    		files[i] = temp;
		}
    	 	
    	for (int i = 0; i<files.length; i++)   //get the latest file for each unique date
    	{	
    	    File lastModifiedFile = (File) files[i].get(0);
    	    for (int j = 1; j < files[i].size(); j++) 
    	    {
    	       if (lastModifiedFile.lastModified() < ((File) files[i].get(j)).lastModified()) 
    	       {
    	           lastModifiedFile = (File) files[i].get(j);
    	       }
    	    }
    	    latestFiles.add(lastModifiedFile);
    	}
	}
	
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException
    {	
    	fileList("D:\\Reports\\Reports2");
    	Dates();
    	uniqueDates();
    	latestFiles();
    	System.out.println("Total number of files = "+latestFiles.size());
    	System.out.println("The files are -");
    	SimpleDateFormat sdf1 = new SimpleDateFormat("dd-MMM-YYYY HH:mm:ss");
    	for(int i=0; i<latestFiles.size(); i++)
    		System.out.println(i+"\t"+latestFiles.get(i)+"\n\tDate = "+sdf1.format(latestFiles.get(i).lastModified())+"\n");
    	
    	
    	for(int i=0; i<latestFiles.size(); i++)
    	{
    		System.out.println(latestFiles.get(i)+"\n"+latestFiles.get(i).lastModified()+"\t"+sdf1.format(latestFiles.get(i).lastModified())+"\n");
    	
    		String sql = " INSERT INTO DEMO ("
        			+ "F_NAME, "
        			+ "L_NAME, "
        			+ "EMAIL, "
        			+ "PHONE, "
        			+ "ZIP CODE, "
        			+ "STATE, "
        			+ "COUNTRY, "
        			+ "DOB, "
        			+ "AGE) VALUES (?,?,?,?,?,?,?,?,?) ";
    		try 
    		{ 
    			Class.forName("oracle.jdbc.driver.OracleDriver");  
    			
    			final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    		    String DB_URL = "jdbc:oracle:thin:@localhost:1521:xe";

    		    // Database credentials
    		    final String USER = "system";
    		    final String PASS = "root";
    	        Connection con = null;  
    	        con = DriverManager.getConnection(DB_URL, USER, PASS);    
    			BufferedReader bReader = new BufferedReader(new FileReader(latestFiles.get(i)));
    			long s1 = latestFiles.get(i).lastModified();
    			String s2 = sdf1.format(latestFiles.get(i).lastModified());
    		    String line = ""; 
    		    bReader.readLine(); // consume first line and ignore
    		    line = bReader.readLine();
    		    while ((line = bReader.readLine()) != null) 
    		    {
    		    	try 
    		        	{
    		            	if (line != null) 
    		                {
    		            		String[] array = line.split(",");
    		                    for(String result:array)
    		                    {
    		                        System.out.println(result);
    		                        PreparedStatement ps = con.prepareStatement(sql);
    		                        ps.setString(1,array[0]);
    		                        ps.setString(2,array[1]);
    		                        ps.setString(3,array[2]);
    		                        ps.setString(4,array[3]);
    		                        ps.setString(5,array[4]);
    		                        ps.setString(6,array[5]);
    		                        ps.setString(7,array[6]);
    		                        ps.setLong(8,s1);
    		                        ps.setString(9,s2);
    		                        ps.executeUpdate();
    		                        ps.close();
    		                        break;
    		                    }
    		                } 
    		            }
    		            finally
    		            {
    		               if (bReader == null) 
    		                {
    		                    bReader.close();
    		                }
    		            }
    		        }
    		    } 
    		catch (FileNotFoundException ex) 
    		{
    		        ex.printStackTrace();
    		}
    	}
    }
}