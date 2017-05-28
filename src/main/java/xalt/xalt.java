package xalt;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class xalt {

//	0, _corrupt_record,allocation,build_date,build_user,date,
//	5, exec_path,field_of_science,host,job_id,linkA,
//	10, link_program,module_name,num_cores,num_nodes,num_threads,
//	15, run_time,start_time,user,

	public static void help(){
		System.out.println("usage:");
		System.out.println("-f (required) path to the data file(s)");
		System.out.println("-b (required) the col index for aggreagting values");
		System.out.println("-v (required) the col index(s) of values to be aggregated, seperated by comma");
		System.out.println("-a Run association analysis");
		System.out.println("-s Run sequential pattern analysis");
		System.out.println("-d Get distribution matrix between bucket index and first value index");
		System.out.println("-sup minimum support value used in association analysis and sequential pattern analysis");
		System.out.println("-conf minimum confidence value used in association analysis");
		System.out.println("-size minimum size of items per transaction in association analysis");
		System.out.println("-len maximum sequences to consider in sequential pattern analysis");
		System.out.println("-out the output directory");
		System.out.println("-debug the level for debug information, 0 is off");

		System.out.println("-h Show help messages");
	}
	//in	a comma seperated list of indexes)
	public static int [] getIndexs(String in){
		String [] sa =in.split(",");
		int [] v = new int[sa.length];
		for (int i=0; i<sa.length; i++)
			v[i]=Integer.parseInt(sa[i]);
		return v;
	}
	
	public static void main (String [] args){
		
		String dataPath="data/all/xalt-2014-12.json";
		//to ignore private executable. 
		String query ="SELECT * from Logs where LENGTH(exec_path)<20 and exec_path IS NOT NULL order by date";
		int bucket=5;//Integer.MIN_VALUE;
		int [] values=new int []{5, 6};//Integer.MAX_VALUE;
		boolean GET_STATISTICS=true;
		boolean RUN_DISTRIBUTION =false;
		boolean RUN_ASSOCIATION=false;
		boolean RUN_SEQPATTERN=false;
		int debug_level=0;
		
		double support=1;
		double confidence=1;
		int min_size=2;
		int max_len=5;
		String output="";
		if (debug_level==0){
			  Logger.getLogger("org").setLevel(Level.OFF);
			  Logger.getLogger("akka").setLevel(Level.OFF);
		}
			
		for (int i=0; i<args.length; i++)
		{
			try{
				if (args[i].equalsIgnoreCase("-f"))
					dataPath=args[++i];
				else if (args[i].equalsIgnoreCase("-s"))
					RUN_SEQPATTERN=true;
				else if (args[i].equalsIgnoreCase("-d"))
					RUN_DISTRIBUTION=true;
				else if (args[i].equalsIgnoreCase("-a"))
					RUN_ASSOCIATION=true;		
				else if (args[i].equalsIgnoreCase("-b"))
					bucket=Integer.parseInt(args[++i]);
				else if (args[i].equalsIgnoreCase("-v"))
					values=getIndexs(args[++i]);
				else if (args[i].equalsIgnoreCase("-sup"))
					support=Double.parseDouble(args[++i]);
				else if (args[i].equalsIgnoreCase("-conf"))
					confidence=Double.parseDouble(args[++i]);
				else if (args[i].equalsIgnoreCase("-size"))
					min_size=Integer.parseInt(args[++i]);
				else if (args[i].equalsIgnoreCase("-len"))
					max_len=Integer.parseInt(args[++i]);
				else if (args[i].equalsIgnoreCase("-debug"))
					debug_level=Integer.parseInt(args[++i]);
				else if (args[i].equalsIgnoreCase("-out"))
					output=args[++i];
				else if (args[i].equalsIgnoreCase("-h"))
					help();
				else {
					System.out.println("Invalid Option: "+args[i]+" use -h for help");
					System.exit(-1);
				}
			}catch(Exception e)
			{
			 	System.out.println("Unrecongnized Option: "+args[i]+" use -h for help");
			 	System.exit(-1);
			}
		}
		
		//System.out.println("Finish parsing the parameters");
		
		if (dataPath==null){
			System.out.println("Require at least one data file.");
			help();
			System.exit(-1);
		}
		
		//LogAnalyzer analyzer=new LogAnalyzer(dataPath, output, new SparkConf().setAppName("XALTLogAnalyzer"));
		LogAnalyzer analyzer=new LogAnalyzer(dataPath, output);
		analyzer.loadLogs();
		Dataset<Row> df = analyzer.getDataFrame(query);
		if (GET_STATISTICS)
			System.out.println(analyzer.getStatistics());
		if (RUN_DISTRIBUTION)
			analyzer.getDistributionMatrix(df.javaRDD(), values[0], bucket);
		if (RUN_ASSOCIATION)
			analyzer.associationAnalysis(df.javaRDD(), values, bucket, min_size, support, confidence);
		if (RUN_SEQPATTERN){
			Dataset<Row> selected = analyzer.sqlContext.sql("SELECT exec_path, field_of_science, date, user "
					+ "from Logs where LENGTH(exec_path)<20 and exec_path IS NOT NULL order by date" );
			LogAnalyzer.runSeqPattern(selected, support, max_len);
		}
	}
	

}
