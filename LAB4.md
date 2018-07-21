LAB 4 - JOINING LARGE DATASETS
Week 6 lab

November 1st 2017

 The objectives of this session are:

Design Map/Reduce flows that join multiple datasets of information.
Use the SequenceFileInput/Output format to efficiently chain the input/output of subsequent Map/Reduce jobs
Implement Joins in the Map/Reduce programming paradigm.

DATASET
Over this lab session we will the use same stock exchange dataset from the last lab. You can refer to lab 3 for documentation details on its format, as well as the DailyStock class. 

Additionally, we will use a second dataset that includes additional information. For each company (identified by the same code appearing in the stock dataset), a row of data includes the following additional information: full name, IPO date, and business sector classification. You can download this second dataset as a TSV (Tab-separated values, \t in Java) file directly from QMPlus,


This file is also available in HDFS for the moonshot cluster, at the path /data/companylist.tsv As a reminder, you can check the contents of a folder by invoking hadoop fs -ls command:


hadoop fs -ls /data

 

DATA ANALYSIS GOAL
The goal of this exercise is to compute, for each year in the dataset, which sector had the highest number of combined operations. The output line must mention the year, name of the sector, and global aggregate of operations. In order to make the exercise more straightforward, we will only compute in the base part for each year, the total number of operations per company. Sample output (not based on the real data) would look similar to:

Finance,1996,20090342 
Pharma,1996,12312312
Finance,1997,25612312
MAP/REDUCE JOB DESIGN
In order to solve this goal you will need to implement two Map/Reduce jobs: the first one will join the stock dataset with the additional details dataset that is provided as a separate file. The second job will take the joined data, and compute the requested aggregate statistics for each year.
Job 1: Takes /data/NASDAQseq, /data/companyList.tsv datasets. Join them to obtain for each record 'sector', 'daily_stock_volume', and 'year'.
Job 2: Aggregate all the daily stock volumes for each company and each year.
JOB 1: JOINING BOTH DATASETS 
The first job will perform the join operation between both datasets.
As we have covered in the lecture, there are multiple ways to implement joins in MapReduce. Review the lecture slides about joins to see the possible alternatives. You can also check Chapter 12.3 of the Hadoop in Practice book (available online here), and Chapter 5 of MapReduce Design Patterns to get more information about the practical aspects of joining in Hadoop.
Based on the sizes of both datasets you want to join, which join strategy would you choose and why?
We recommend using a Map-side replication join, where Hadoop's Distributed Cache is used for loading the smaller dataset. The cache needs to be first configured in the job configuration, selecting the data that will be shared across all the Mappers independently. This is a Map-only job. The following code contains a complete implementation of a Job configuration, with only a Map function, and the DistributedCache loading the additional tsv file into all Mappers. 

We provide a full implementation of this job as part of the lab materials (check lab4-part1.zip in QMPlus). Create a project with these files, and have a look at how the join has been implemented. First, open the job class (StockCompanyReplJoin):


public class StockCompanyReplJoin {
   public static void runJob(String[] input, String output) throws Exception {

     Job job = Job.getInstance(new Configuration());
     Configuration conf = job.getConfiguration();
     job.setJarByClass(StockCompanyReplJoin.class);
     job.setMapperClass(StockCompanyReplJoinMapper.class);
     //note the job has no Reducer
     job.setInputFormatClass(StockInputFormat.class);
     job.setOutputKeyClass(TextIntPair.class);
     job.setOutputValueClass(LongWritable.class);
     job.setOutputFormatClass(SequenceFileOutputFormat.class);

     job.addCacheFile(new Path("/data/companylist.tsv").toUri());

     Path outputPath = new Path(output);
     FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
     FileOutputFormat.setOutputPath(job, outputPath);
     outputPath.getFileSystem(conf).delete(outputPath, true);
     job.waitForCompletion(true);
   }
  
   public static void main(String[] args) throws Exception {
     runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
   }
}
The job configuration defines a SequenceFileOutputFormat, (similar to the one from /data/NASDAQseq). This presents two advantages when chaining jobs: the information will be encoded more efficiently, and the Mapper from the next job will be able to read the input arbitrary Writables for Key, and Value objects. You can see from the job declaration that we are using another custom Writable as an output for this job: TexIntPair. This is a simple Writable that holds two Writables, a Text and an IntWritable. You can access either of them by invoking the methods .getLeft(), .getRight(), set(). Feel free to check the implementation of the class for further information.

Now have a look at the Mapper class (StockReplJoinMapper). This Mapper has a new method: setup(Context context). This method is invoked by Hadoop once during initialisation of the job. In our case, we use this mechanism to load all the company names and sector for each company in a Hashtable.


  protected void setup(Context context) throws IOException, InterruptedException {

        companyInfo = new Hashtable<String, String>();

        // We know there is only one cache file, so we only retrieve that URI
        URI fileUri = context.getCacheFiles()[0];

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(fileUri));

        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line = null;
        try {
            // we discard the header row
            br.readLine();

            while ((line = br.readLine()) != null) {
                context.getCounter(CustomCounters.NUM_COMPANIES).increment(1);

                String[] fields = line.split("\t");
                // Fields are: 0:Symbol 1:Name 2:IPOyear 3:Sector 4:industry
                if (fields.length == 5)
                    companyInfo.put(fields[0], fields[3]);
            }
            br.close();
        } catch (IOException e1) {
        }

        super.setup(context);
    }
The Map method uses the information filled in the Hashtable companyInfo to retrieve the sector belonging to each company. By looking at the code, figure out exactly what information is being sent to the second MapReduce job.Compile the jar from the provided classes and run the job. The input path should be /data/NASDAQseq, and you can specify a new folder in your home HDFS space for saving the results. While you won't be able to read the results (they are encoded in a SequenceFile, you can check the number of emitted records in the Mapper to guess whether the job is working correctly.

JOB 2: COMPUTING THE TOTAL SECTORIAL MOVEMENT EACH YEAR
The second job is a simple aggregate computation of the values we selected with the previous join. Repeat the same process from past lab sheets (identify feature, and values to be computed in the Mapper. Perform computations over the aggregated values in the Reducer) in order to complete the lab. 

For the job, you will need to use the SequenceFileInputFormat to match the format generated by your previous job. This way, the key and value types for this Mapper will be the same key and value types emitted by the previous job.

Remember you have to create in your new project three classes: A Job definition, a Mapper, and a Reducer. In the job definition, reason about the following two decisions: Is it worth adding a Combiner to this job? If true, can you use the same Reducer function as a Combiner? How many Reducers you want to use ideally in order to obtain a single set of results ordered by sector and year?
Complete the implementation of this job and execute it, setting as input path the output path of the first Map/Reduce job.
You could add now an additional MapReduce job that computes the maximum for each year across all sectors. What would be the features in this case? And what data do you need to compute the maximum in the Reducer?
Now you have completed the job, would it be possible to execute all of it within a single Map/Reduce job?
.
OPTIONAL EXERCISE
Compute, for each company and business sector, which company grew the most per year, listing as well the growth percentage. Results should be in a format similar to:

Finance,1996,ABCD,46%
Finance,1997,VFER,64%
With these two lines meaning that, in the year 1996 the company ABCD was the one who grew the most among the finance business sector, with 46% growth between the initial and final values. Similarly, VFER was the leader in the finance sector in 1997, with a total growth of 64%.

You might need to implement multiple MapReduce jobs,including a join job for combining multiple parts of data.
