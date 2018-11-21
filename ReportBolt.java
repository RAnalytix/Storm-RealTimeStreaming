import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.sql.*; 
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.io.*;

public class ReportBolt extends BaseRichBolt {

	private OutputCollector collector;
	private HashMap<String, Long> ReportCounts = null;
	int temp_count_variable=0;
	private Connection con;
	private Statement stmt;
	static final String username="root";
	//static final String password="root";
	static final String password="123";
	public void prepare(Map config, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		this.ReportCounts = new HashMap<String, Long>();
		//code added to make connection to mysql database.
		try 
		{
			Class.forName("com.mysql.jdbc.Driver");
			con=DriverManager.getConnection("jdbc:mysql://localhost:3306/upgrad",username,password);
			stmt=con.createStatement(); 
		} catch ( SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		catch (ClassNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}

	//I am finally going to receive all these tuples in Report Bolt's single instance through global streaming. 
	//This ensures that all the incoming tuples with (word,1L) from wordcount bolt are reaching Report Bolt. 
	//Hence, the word count logic has been moved to the Report bolt.
	
	public void execute(Tuple tuple) 
	{
		
		//Below lines of code until put(word, newCount) perform the following steps for counting the frequency of each word:
		//1. It gets the word (for e.g. - 'beverages') and checks if it is already present in the HashMap.
		//2. If yes , it takes the value (count) for this key(word) from the Hashmap and adds the  
		//current count which is (1L) from the incoming tuple. This increments the word count in the Hashmap.
		//3. If not , it adds the new count (1L) for the incoming word in the Hashmap.

		try {
			String word = tuple.getStringByField("word");
			Long count = tuple.getLongByField("count");
			Long newCount = 0L;
			if(this.ReportCounts.containsKey(word)) {
				newCount = this.ReportCounts.get(word) + count;

			}
			else {

				newCount = count;
			}
			this.ReportCounts.put(word, newCount);
			
			//This is a temp variable created whose value is incremented each time an incoming tuple is executed.
			temp_count_variable++;
			//Queries to insert into database
			String DeleteQuery= "DELETE FROM wordcounts";
			
			//Every time 1000 tuples have been executed, database is updated appropriately.
			if(temp_count_variable==1000)
			{
				temp_count_variable=0;
				try 
				{
					System.out.println("Database clearing");
					
					//The existing records from table are deleted.
					stmt.executeUpdate(DeleteQuery);
					System.out.println("Database cleared");
				} catch (SQLException e1) {
					e1.printStackTrace();
				} 
				
				//All the keys are then added to a key set where sorting is performed. 
				//This is the reason why we have our results sorted alphabetically.
				
				List<String> keys = new ArrayList<String>();
				keys.addAll(this.ReportCounts.keySet());
				Collections.sort(keys);
				for (String key : keys)
				{

					//Each key is then inserted into table - wordcounts such that every record corresponds to a single word 
					//with its total occurrences in the input stream
					//The count of each word is extracted from the hashMap where we are maintaining the total count for each word.
					
					String InsertQuery= "INSERT INTO wordcounts(word,count) VALUES('"+key+"',"+this.ReportCounts.get(key)+")";// ON DUPLICATE KEY UPDATE count="+this.ReportCounts.get(key);    

					try 
					{
						System.out.println("Database Updating");
						boolean rs=stmt.execute(InsertQuery);
						System.out.println("Database Updated");
					} catch (SQLException e)
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

			}
			
			//Once DB update is completed , an ack is sent to sentence spout.
		
			this.collector.ack(tuple);
		} catch(Exception e) {
			
			//If any exception occurs , a fail is sent to sentence spout.
			this.collector.fail(tuple);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// this bolt does not emit anything
	}

	@Override
	public void cleanup() {
		System.out.println("--- FINAL COUNTS ---");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.ReportCounts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println(key + " : " + this.ReportCounts.get(key));
		}
		System.out.println("--------------");
	}
}
