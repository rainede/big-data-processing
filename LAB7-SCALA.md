LAB 7 - APACHE SPARK (SCALA)
The objectives of this lab are:

Familiarise with the concepts of Spark RDDs
Learn how to interact with HDFS within Spark
Write interactive Spark flows using the spark-shell
Use basic transformations to execute RDD computations
Use actions to diagnose the current status of RDDs
Persist RDDs in memory to start interactive queries of the data
Submitting jobs to the cluster

INTRODUCTION
For this lab we will interact with Spark using exclusively the interactive Spark Shell, and writing commands in Scala. This shell works as a CLI (Command Line Interpreter) of Scala instructions.

You can start the spark shell by invoking from the ITL the command spark-shell. After several lines of debugging information, you will see a message prompt such as 

scala>
The shell creates a Scala Context object, sc, that can be invoked in order to create the initial RDDs for your computation. 

RDD CREATION AND EXPLORATION WITH ACTIONS
We will create our first RDD by reading the dataset from our lab 2 in Scala (the project Gutenberg files). In order to do so, we need to invoke the textFile method from the sc, and save the resulting RDD in a Scala variable (val):

val lines = sc.textFile("/data/gutenberg")
As you can see, in order to load HDFS files we need to specify the url of the HDFS namenode (in our case moonshot-ha-nameservice resolves to it within QMUL network).

Once the command finishes you will see a reference to the RDD. However, the RDD object does not exist yet. Spark RDDs are only materialised whenever we request a result from them. In order to do so, we will use one action, that returns some information from the RDD to our driver program. This involves network transfer, and loads the memory of the single driver machine. Let's invoke the count action on the RDD, which will return to the driver the number of elements in the RDD.

lines.count()
You will see several lines in the command line showing the operations triggered by your command, including the actual creation of the RDD from HDFS. You will finally see the result of your count action in the console. Additionally, by looking at the previous lines, you should be able to get some insight on what is happening in the cluster because of your command.

How many lines of text appear in the gutenberg dataset?
Based on what you can find from the logs, how many splits does the 'lines' RDD have? Remember this structure will be distributed among multiple nodes of your cluster.
In order to have a look at a fraction of the RDD we can use a different action. takeSample collects a random sample of elements from the requested RDD. The following line will provide 5 entries back to the driver program. 

val sample = lines.takeSample(false, 5)
When running the takeSample command you will see that again a set of transformations have been triggered.

Compare the output of this operation with the one of the count before. Based on what you see, do you think the lines RDD is shared between two operations, or is it created again each time?
Note that sample is a local variable inside the driver program. We can write any Scala code to manipulate it, and obtain any result from it.  For example, we can use sample.foreach(println), to print in the screen each element as a line of text. This is another example of Scala's functional programming approach (executing the provided function for each element from the array). 

IMPLEMENTATION OF WORD COUNT
At this point we will recreate the original lab 1/2 exercise, counting how many times each word appears in the dataset. 

Spark defines multiple types of transformations. They are similar to Hadoop' s Map or Reduce commands, but they have a more specific focus. Transformations are applied to one or two elements of the input collection at the same time, and will generate as an output a new transformation. In addition to the slides, the spark programming guide provides a good overview of the characteristics of each transformation. http://spark.apache.org/docs/latest/programming-guide.html#transformations . We will use three different transformations to implement Wordcount:

flatMap: applies function to each element in collection. Generates one or more results from each initial element
map: applies function to each element in collection. Generates exactly one result from each initial element
reduceByKey: combines items in the RDD (which must be a tuple with key values), according by key. Then, for each set of values for the same key, applies the provided binary function iteratively. The result will be an RDD with the set of unique keys, and one aggregated value per key. 
We provide you the three different functions that you will need to provide as arguments for the three transformations. The first one splits a String into a set of Strings where the delimiter spaces are ;:, () - the argument of the function is a regular expression. The second one creates a pair of elements from each input element, with the first one being the original element, and the second the integer 1. The third one adds two elements. Note that you cannot type these functions as they are on the command line. They are meant to be arguments for Spark transformations.

line => line.split("[ .,;:()]+"))
word => (word,1)
(a,b) => (a+b)
Using these three transformations, define an RDD with the count of occurrences for each word, and save it in a val. You can define intermediate variables for naming the RDDs generated after each transformation. You will need to use the three transformations we were mentioning before (map, flatMap and ReduceByKey).

val step1 = lines.______(<<function1>>)
val step2 = step1.______(<<function2>>)
val result = step2.______(<<function3>>)
Once you have completed the RDD dataflow, answer the following question from your Spark shell: how many different words appear in the dataset?
CACHING AND SAVING THE RESULTS
To finalise the lab we are going to explore how Spark allows us to interact with the obtained results in multiple ways.

First, we will take advantage of Spark's caching functionality to make an RDD persistent in memory. We do that by invoking the persist transformation.

val inmem = result.persist()
Persisted RDDs are kept in memory after executing the first time. This means that multiple transformations and actions over them will be executed substantially faster, as there is no need to recreate them from HDFS, as well as previous transformation steps. We will take advantage of that to interact with it, checking what is the count for several words. 

We will use the filter transformation (see Spark documentation) to select only one element from the results. As we want to get the count for a specific word, we need to select the item from the RDD whose key is the word we are looking for. We can select the first element of a Scala tuple with the expression tuple._1 as shown in the code below.

Additionally, we need to invoke an action after our transformation, as otherwise the single value will be still distributed in the cluster. By invoking collect on the filtered RDD we will get back the result we are looking for to our driver program. The following Spark instruction will therefore return the value we are looking for:

inmem.filter(pair=>pair._1.equals("summer")).collect()
How many times is summer mentioned in the dataset? Modify now this last query (you can push the up arrow in the keyboard to reload the last command for quickly editing), and check the popularity of other words. You should see substantially improved performance for all these subsequent invocations. This is thanks to the persist() transformation we invoked early.  
Finally, you can save any RDD back to HDFS by invoking the saveAsTextFile action (if the desired output format is TextOutputFormat). You have to specify the absolute path inside hdfs for that, for which you need to know what is your user account in HDFS.

inmem.saveAsTextFile("lab7/output.txt")
