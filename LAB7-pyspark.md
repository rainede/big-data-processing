LAB 7 - APACHE SPARK (PYTHON)
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

You can start the Python spark shell by invoking from the ITL the command pyspark. After several lines of debugging information, you will see a message prompt such as 

>>>
The shell creates a Scala Context object, sc, that can be invoked in order to create the initial RDDs for your computation. 

RDD CREATION AND EXPLORATION WITH ACTIONS
We will create our first RDD by reading the dataset from our lab 2 in Python(the project Gutenberg files). In order to do so, we need to invoke the textFile method from the sc, and save the resulting RDD in a  variable called lines:

lines = sc.textFile("/data/gutenberg")
As you can see, we can specify paths inside HDFS, using the same convention of the input and output of our Map/Reduce jobs.  

Once the command finishes you will see a reference to the RDD. However, the RDD object does not exist yet. Spark RDDs are only materialised whenever we request a result from them. In order to do so, we will use one action, that returns some information from the RDD to our driver program. This involves network transfer, and loads the memory of the single driver machine. Let's invoke the count action on the RDD, which will return to the driver the number of elements in the RDD.

lines.count()
You will see several lines in the command line showing the operations triggered by your command, including the actual creation of the RDD from HDFS. The different stages of execution are represented with something like:

[Stage 0:>                                                          (0 + 0) / 4]

Additionally, for each stage, you will see an estimation of how many parallel tasks it consist of. At the end of the execution, the terminal will return the number of lines of the input.

How many lines of text appear in the gutenberg dataset?
Based on what you can find from the logs, how many splits does the 'lines' RDD have? Remember this structure will be distributed among multiple nodes of your cluster.
In order to have a look at a fraction of the RDD we can use a different action. takeSample collects a random sample of elements from the requested RDD. The following line will provide 5 entries back to the driver program. 

sample = lines.takeSample(False, 5)
When running the takeSample command you will see that again a set of transformations have been triggered.

Compare the output of this operation with the one of the count before. Based on what you see, do you think the lines RDD is shared between two operations, or is it created again each time?
Note that sample is a local variable inside the driver program. We can write any Python code to manipulate it, and obtain any result from it.  For example, we can use the following loop to print every line in the returned array as a line of text for line in sample: print(line) . 

IMPLEMENTATION OF WORD COUNT
At this point we will recreate the original lab 1/2 exercise, counting how many times each word appears in the dataset. 

Spark defines multiple types of transformations. They are similar to Hadoop' s Map or Reduce commands, but they have a more specific focus. Transformations are applied to one or two elements of the input collection at the same time, and will generate as an output a new transformation. In addition to the slides, the spark programming guide provides a good overview of the characteristics of each transformation. http://spark.apache.org/docs/latest/programming-guide.html#transformations . We will use three different transformations to implement Wordcount:

flatMap: applies function to each element in collection. Generates one or more results from each initial element
map: applies function to each element in collection. Generates exactly one result from each initial element
reduceByKey: combines items in the RDD (which must be a tuple with key values), according by key. Then, for each set of values for the same key, applies the provided binary function iteratively. The result will be an RDD with the set of unique keys, and one aggregated value per key. 
We provide you the three different functions that you will need to provide as arguments for the three transformations. 

The first one splits a String into a set of Strings where the delimiter spaces are<space>;:,.+-*/ () - the first argument of the function is a regular expression. 

The second one creates a pair of elements from each input element, with the first one being the original element, and the second the integer 1. 

The third one adds two elements. Note that you cannot type these functions as they are on the command line. They are meant to be arguments for Spark transformations.

lambda line: re.split("[ .,;:()-+*]",line) 
lambda word:  (word,1)
lambda a,b: (a+b)
Using these three transformations, define an RDD with the count of occurrences for each word, and save it in a val. You can define intermediate variables for naming the RDDs generated after each transformation. You will need to use the three transformations we mentioned before (map, flatMap and reduceByKey).

import re # we need this import to use the regular expressions function of the first lambda 
step1 = lines.______(<<lambda 1>>)
step2 = step1.______(<<lambda 2>>)
result = step2.______(<<lambda 3>>)
Once you have completed the RDD dataflow, answer the following question from your Spark shell: how many different words appear in the dataset?
CACHING AND SAVING THE RESULTS
To finalise the lab we are going to explore how Spark allows us to interact with the obtained results in multiple ways.

First, we will take advantage of Spark's caching functionality to make an RDD persistent in memory. We do that by invoking the persist transformation.

inmem = result.persist()
Persisted RDDs are kept in memory after executing the first time. This means that multiple transformations and actions over them will be executed substantially faster, as there is no need to recreate them from HDFS, as well as previous transformation steps. We will take advantage of that to interact with it, checking what is the count for several words. 

We will use the filter transformation (see Spark documentation) to select only one element from the results. As we want to get the count for a specific word, we need to select the item from the RDD whose key is the word we are looking for. We can select the first element of a Python tuple with the expression tuple[0] as shown in the code below.

Additionally, we need to invoke an action after our transformation, as otherwise the single value will be still distributed in the cluster. By invoking collect on the filtered RDD we will get back the result we are looking for to our driver program. The following Spark instruction will therefore return the value we are looking for:

inmem.filter(lambda pair:pair[0] == "summer").collect()
How many times is summer mentioned in the dataset? Modify now this last query (you can push the up arrow in the keyboard to reload the last command for quickly editing), and check the popularity of other words. You should see substantially improved performance for all these subsequent invocations. This is thanks to the persist() transformation we invoked early.  
Finally, you can save any RDD back to HDFS by invoking the saveAsTextFile action (if the desired output format is TextOutputFormat). You have to specify the absolute path inside hdfs for that, for which you need to know what is your user account in HDFS.

inmem.saveAsTextFile("lab7/output.txt")
Exit the Python terminal with exit(), and check that the files were saved correctly by invoking the usual hdfs fs commands we have previously used. Make sure to delete the whole folder (hadoop fs -rmr lab7) once you have finished checking the files, in order to fre
