package edu.drexel.cs461.apriori;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 
 * @author Thomas Cuevas
 * @author Jon Thompson 
 * Frequent itemset mining using Apache Spark SQL.
 *
 */
public final class Apriori {
    
    private static JavaSparkContext sparkContext;
    private static SQLContext sqlContext;
    
    /**
     * Set up Spark and SQL contexts.
     */
    private static void init (String master, int numReducers) {
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		SparkConf sparkConf = new SparkConf().setAppName("Apriori")
		    .setMaster(master)
		    .set("spark.sql.shuffle.partitions", "" + numReducers);
		
		sparkContext = new JavaSparkContext(sparkConf);
		sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
	    }
    
    /**
     * 
     * @param inFileName
     * @return
     */
    private static DataFrame initXact (String inFileName) {
		
		// read in the transactions file
		JavaRDD<String> xactRDD = sparkContext.textFile(inFileName);
		
		// establish the schema: XACT (tid: string, item: int)
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("tid", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("item", DataTypes.IntegerType, true));
		StructType xactSchema = DataTypes.createStructType(fields);
	
		JavaRDD<Row> rowRDD = xactRDD.map(
						  new Function<String, Row>() {
						      static final long serialVersionUID = 42L;
						      public Row call(String record) throws Exception {
							  String[] fields = record.split("");
							  return  RowFactory.create(fields[0], Integer.parseInt(fields[1].trim()));
						      }
						  });
	
		// create DataFrame from xactRDD, with the specified schema
		return sqlContext.createDataFrame(rowRDD, xactSchema);
    }
    
    private static void saveOutput (DataFrame df, String outDir, String outFile) throws IOException {
	
		File outF = new File(outDir);
	        outF.mkdirs();
	        BufferedWriter outFP = new BufferedWriter(new FileWriter(outDir + "/" + outFile));
	            
		List<Row> rows = df.toJavaRDD().collect();
		for (Row r : rows) {
		    outFP.write(r.toString() + "\n");
		}
	        
	        outFP.close();

    }
    
    public static void main(String[] args) throws Exception {
	
		if (args.length != 5) {
		    System.err.println("Usage: Apriori <inFile> <support> <outDir> <master> <numReducers>");
		    System.exit(1);
		}
	
		String inFileName = args[0].trim();
		double thresh =  Double.parseDouble(args[1].trim());
		String outDirName = args[2].trim();
		String master = args[3].trim();
		int numReducers = Integer.parseInt(args[4].trim());
	
		Apriori.init(master, numReducers);
		DataFrame xact = Apriori.initXact(inFileName);
		
		// compute frequent pairs (itemsets of size 2), output them to a file
		DataFrame frequentPairs = computeFrequentDoubles(xact, thresh);
		
		try {
		    Apriori.saveOutput(frequentPairs, outDirName + "/" + thresh, "pairs");
		} catch (IOException ioe) {
		    System.out.println("Cound not output pairs " + ioe.toString());
		}
	
		// compute frequent triples (itemsets of size 3), output them to a file
		DataFrame frequentTriples = computeFrequentTriples(xact, thresh);
		
		try {
		    Apriori.saveOutput(frequentTriples, outDirName + "/" + thresh, "triples");
		} catch (IOException ioe) {
		    System.out.println("Cound not output triples " + ioe.toString());
		}
		
		sparkContext.stop();
	        
    }
    
    private static DataFrame computeFrequentDoubles(DataFrame transactions, double minsup)
    {
    	return computeApriori(transactions, 2, minsup);
    }
    
    private static DataFrame computeFrequentTriples(DataFrame transactions, double minsup)
    {
    	return computeApriori(transactions, 3, minsup);
    }
    
    private static DataFrame computeApriori(DataFrame transactions, int maxK, double minsup)
    {
    	Itemsets F = new Itemsets(maxK);
    	long n = transactions.count();
    	
    	F.set(1, computeOneItemsets(transactions));
    	
    	for (int k = 2; F.get(k - 1).count() > 0 && k <= maxK; k++) {
    		DataFrame C = candidateGen(F.get(k - 1));
    		
    		for (Row t : transactions.toJavaRDD().collect()) {
        		for (Row c : C.toJavaRDD().collect()) {
        			
        		}			
    		}
    		
    		F.set(k, C.filter(C.col("count").gt(minsup * n)));
    	}
    	
    	return F.get(maxK);
    }

	private static DataFrame candidateGen(DataFrame dataFrame) {
		return null;
	}

	private static DataFrame computeOneItemsets(DataFrame transactions) {
		return null;
	}
	
	private static class Itemsets
	{
		private DataFrame[] dataFrames;
		
		public Itemsets(int number)
		{
			dataFrames = new DataFrame[number];
		}
		
		public DataFrame get(int i)
		{
			return dataFrames[i - 1];
		}
		
		public void set(int i, DataFrame dataFrame)
		{
			dataFrames[i - 1] = dataFrame;
		}
	}
}
