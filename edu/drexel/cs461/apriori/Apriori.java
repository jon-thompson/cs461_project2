package edu.drexel.cs461.apriori;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.Seq;
import static org.apache.spark.sql.functions.*;

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
							  String[] fields = record.split("\t");
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
		//DataFrame frequentTriples = null;
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
    	long numTransactions = transactions.groupBy("tid").count().count();
    	DataFrame transactionItemSets = getTransactionIDs(transactions);

		HashMap<String, ArrayList<Integer>> itemsMap = getTransactionsMap(transactionItemSets, transactions);

    	F.set(1, computeOneItemsets(transactions));
    	
    	for (int k = 2; k <= maxK && F.get(k - 1).count() > 0; k++) {
    		DataFrame C = candidateGen(F.get(k - 1));
    		
    		for (Row t : transactionItemSets.toJavaRDD().collect()) {	
    			String tid = t.getString(0);
    			JavaRDD<Row> rows = C.toJavaRDD().map(new Function<Row, Row>() {
    				public Row call(Row row) {
    					Seq<Integer> candidateItems = row.getSeq(0);
    					
    					if (checkCandidates(itemsMap.get(tid), candidateItems)) {
    						//update the count
    						long count;
    						try {
    							count = row.getLong(1) + 1;
    						} catch (Exception e) {
    							count = 0;
    							System.out.println("Couldn't retrieve count");
    						}  
            				return RowFactory.create(candidateItems.toList(), count);        				
            			}
    					return row;
    				}
    			});

    			C = sqlContext.createDataFrame(rows, C.schema());
    			//hack to force evaluation of the filter
    			C.show();
    		}
    		
       		F.set(k, C.filter(C.col("count").gt(new Long((long) (minsup * numTransactions)))));

    	}
    	
    	return F.get(maxK);
    }

	private static DataFrame candidateGen(DataFrame F) {
		DataFrame p = F.select("items").toDF("p");
		DataFrame q = F.select("items").toDF("q");
		
		JavaRDD<Row> rows = p.join(q)
				.toJavaRDD()
				.map(new Function<Row, Row>() {
					public Row call(Row row) {
						Seq<Integer> pArray = row.getSeq(0);
						Seq<Integer> qArray = row.getSeq(1);
						int k = pArray.length();
						
						for (int i = 0; i < k - 1; i++) {
							if (pArray.apply(i) != qArray.apply(i))
								return null;							
						}
						
						if (pArray.apply(k - 1) >= qArray.apply(k - 1))
							return null;
						
						int[] items = new int[k + 1];
						
						for (int i = 0; i < k; i ++)
							items[i] = pArray.apply(i);
						
						items[k] = qArray.apply(k-1);
						
					    return RowFactory.create(items, new Long(0));
					 }
				})
				.filter(new Function<Row, Boolean>() {
					public Boolean call(Row row) {
						return row != null;
					}
				});
		
		DataFrame C = sqlContext.createDataFrame(rows, F.schema());
		
		C.show();
		
		return C;
	}

	private static DataFrame computeOneItemsets(DataFrame transactions) {
		JavaRDD<Row> rows = transactions
				.groupBy(col("item"))
				.count()
				.toJavaRDD()
				.map(new Function<Row, Row>() {
					  public Row call(Row row) {
					    return RowFactory.create(new int[] { row.getInt(0) }, row.getLong(1));
					  }
					});
		
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("items", DataTypes.createArrayType(DataTypes.IntegerType), true));
		fields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
		StructType schema = DataTypes.createStructType(fields);
		
		return sqlContext.createDataFrame(rows, schema);
	}
	
	//get unique transaction IDs
	private static DataFrame getTransactionIDs(DataFrame transactions) {
		return transactions.select("tid").distinct();
	}
	
	//get the items in a particular transaction
	private static DataFrame getItemsForTransaction(DataFrame transaction, String tid) {
		return transaction.filter(transaction.col("tid").equalTo(tid)).select(transaction.col("item"));
	}
	
	private static HashMap<String, ArrayList<Integer>> getTransactionsMap(DataFrame ids, DataFrame transactions) {
		HashMap<String, ArrayList<Integer>> map = new HashMap<String, ArrayList<Integer>>();
		
		for (Row r : ids.toJavaRDD().collect()) {
			ArrayList<Integer> transactionItems = new ArrayList<Integer>();
			String tid = r.getString(0);
			
			DataFrame tItems = getItemsForTransaction(transactions, tid);
			for (Row row : tItems.toJavaRDD().collect()) {
				transactionItems.add(row.getInt(0));
			}
			map.put(tid, transactionItems);
		}
		
		return map;
	}
	
	//check if all candidate items are present in a given transaction
	private static Boolean checkCandidates(ArrayList<Integer> transactionItems, Seq<Integer> candidateItems) {
		ArrayList<Integer> candidates = new ArrayList<Integer>();
		int numCandidates = candidateItems.length();
		
		for (int i = 0; i < numCandidates; i++) {
			candidates.add(candidateItems.apply(i));
		}
		
		for (int i : candidates) {
			if (!transactionItems.contains(i)) {
				return false;
			}
		}
		
		return true;
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
