# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import time
from pyspark.streaming import StreamingContext
import json

# accum = sc.accumulator(0)
# accum1 = sc.accumulator(0)


# ------------------------------------------
# FUNCTION my_split - extract from dictionary the cuisine, points and evaluation
# ------------------------------------------
def my_split(x):
  
  cuisine = x["cuisine"]
  
  evaluation = x["evaluation"]
  
  points = x["points"]
  
  return (cuisine, (points, evaluation))

# ------------------------------------------
# FUNCTION my_reduce - at 1 to each review and neg review while also adding or subtracting points
#Depending if negative or positive.
# ------------------------------------------
def my_filter(x):
  
  my_tuple = x[1]
  cuisine = x[0]
  points = 0
  numReviews = 1
  numNegReviews = 0  
  
  if my_tuple[1] == "Negative":
    numNegReviews = 1
    points -= my_tuple[0]
  else:
    points = points + my_tuple[0]
  
  
  return (cuisine,(numReviews, numNegReviews, points))
 
# ------------------------------------------
# FUNCTION my_remove - removing ones that dont pass conditions
# ------------------------------------------
def my_remove(x, percentage_f):
  
  reviews = x[1][0]
  numNegReviews = x[1][1]
  average_reviews = x[1][4]
  print("average: " + str(average_reviews))
  percentage_bad_reviews = float((float(numNegReviews)/float(reviews)) * 100)
  
  if reviews >= average_reviews and percentage_bad_reviews < float(percentage_f):    
    return True
  else:
    return False
  
# ------------------------------------------
# FUNCTION my_sort - sorting the RDD while adding average points
# ------------------------------------------
# def my_sort(x):
  
#   cuisine = x[0]
#   reviews = x[1][0]
#   numNegReviews = x[1][1]
#   points = points = x[1][2]  
#   average_points_per_view = float(float(points)/float(reviews))
  
#   return (cuisine, (reviews, numNegReviews, points, average_points_per_view ))


def test_remove(x, percentage_f):
  
#   ('line', (((70, 4, 434), u'American '), 6))

  cuisine = x[1][0][1]
  reviews = x[1][0][0][0]
  numNegReviews = x[1][0][0][1]
  points = x[1][0][0][2]
  average_reviews = x[1][1]
  print("average: " + str(average_reviews))
  percentage_bad_reviews = float((float(numNegReviews)/float(reviews)) * 100)
  average_points_per_view = float(float(points)/float(reviews))
  
  if reviews >= average_reviews and percentage_bad_reviews < float(percentage_f):    
    return (cuisine, (reviews, numNegReviews, points, average_points_per_view))
  else:
    return "empty"
  
  

# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(ssc, monitoring_dir, result_dir, percentage_f):
  
  inputRDD = ssc.textFileStream(monitoring_dir)
    
  dictionaryRDD = inputRDD.map(lambda x: json.loads(x))
    
  splitRDD = dictionaryRDD.map(lambda x: my_split(x))  
  
  mapRDD = splitRDD.map(lambda x: my_filter(x))
  mapRDD.cache()
  
  filterRDD = mapRDD.reduceByKey(lambda x, y: tuple(map(sum, zip(x, y))))
    
  #SORT
  transFilterRDD = filterRDD.transform(lambda x: x.sortBy(lambda x: x[1][0], False))
  
  reviews_count = mapRDD.count().map(lambda x: ("line", x))
  cuisines_count = filterRDD.count().map(lambda x: ("line", x))
  
  #("line", (243, 24))
  joined = reviews_count.join(cuisines_count)
  #("line", 6)

  
  averages = joined.map(lambda x: ("line", (x[1][0] / x[1][1])))
  
#   reviews_map = transFilterRDD.transform(lambda x: x.map( lambda y: ("line", (y[1], y[0]))))
  reviews_map = transFilterRDD.map(lambda x: ("line", (x[1], x[0])))
  reviews_join = reviews_map.join(averages)
  
#   ('line', (((70, 4, 434), u'American '), 6))

#   newRDD = transFilterRDD.map(lambda x: x.map(lambda y: my_transform(x, y)))
  
  removeRDD = reviews_join.map(lambda x: test_remove(x, percentage_f)).filter(lambda x: x != "empty")
  
#   removeRDD = reviews_join.map(lambda x: x[1][1])
#   sortRDD = removeRDD.map(lambda x: my_sort(x))
  
  #SORT
  transSortRDD = removeRDD.transform(lambda x: x.sortBy(lambda x: x[1][3], False))
  #Save to text files
  transSortRDD.pprint()
#   transSortRDD.saveAsTextFiles(result_dir)
#   transSortRDD.pprint()
  #for item in sortRDD.take(10):
    #print(item)
    
    
  pass
  
    

# ------------------------------------------
# FUNCTION create_ssc
# ------------------------------------------
def create_ssc(monitoring_dir, result_dir, max_micro_batches, time_step_interval, percentage_f):
    # 1. We create the new Spark Streaming context.
    # This is the main entry point for streaming functionality. It requires two parameters:
    # (*) The underlying SparkContext that it will use to process the data.
    # (**) A batch interval, specifying how often it will check for the arrival of new data,
    # so as to process it.
    ssc = StreamingContext(sc, time_step_interval)

    # 2. We configure the maximum amount of time the data is retained.
    # Think of it: If you have a SparkStreaming operating 24/7, the amount of data it is processing will
    # only grow. This is simply potato unaffordable!
    # Thus, this parameter sets maximum time duration past arrived data is still retained for:
    # Either being processed for first time.
    # Being processed again, for aggregation with new data.
    # After the timeout, the data is just released for garbage collection.

    # We set this to the maximum amount of micro-batches we allow before considering data
    # old and dumping it times the time_step_interval (in which each of these micro-batches will arrive).
    ssc.remember(max_micro_batches * time_step_interval)

    # 3. We model the ssc.
    # This is the main function of the Spark application:
    # On it we specify what do we want the SparkStreaming context to do once it receives data
    # (i.e., the full set of transformations and ouptut operations we want it to perform).
    my_model(ssc, monitoring_dir, result_dir, percentage_f)

    # 4. We return the ssc configured and modelled.
    return ssc


# ------------------------------------------
# FUNCTION get_source_dir_file_names
# ------------------------------------------
def get_source_dir_file_names(source_dir, verbose):
    # 1. We create the output variable
    res = []

    # 2. We get the FileInfo representation of the files of source_dir
    fileInfo_objects = dbutils.fs.ls(source_dir)

    # 3. We traverse the fileInfo objects, to get the name of each file
    for item in fileInfo_objects:
        # 3.1. We get a string representation of the fileInfo
        file_name = str(item)
        if verbose == True:
            print(file_name)

        # 3.2. We look for the pattern name= to remove all useless info from the start
        lb_index = file_name.index("name=u'")
        file_name = file_name[(lb_index + 7):]

        # 3.3. We look for the pattern ') to remove all useless info from the end
        ub_index = file_name.index("',")
        file_name = file_name[:ub_index]

        # 3.4. We append the name to the list
        res.append(file_name)
        if verbose == True:
            print(file_name)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION streaming_simulation
# ------------------------------------------
def streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose):
    # 1. We get the names of the files on source_dir
    files = get_source_dir_file_names(source_dir, verbose)

    # 2. We simulate the dynamic arriving of such these files from source_dir to dataset_dir
    # (i.e, the files are moved one by one for each time period, simulating their generation).
    for file in files:
        # 2.1. We copy the file from source_dir to dataset_dir
        dbutils.fs.cp(source_dir + file, monitoring_dir + file, False)

        # 2.2. We wait the desired transfer_interval
        time.sleep(time_step_interval)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            percentage_f):
    # 1. We setup the Spark Streaming context
    # This sets up the computation that will be done when the system receives data.
    ssc = StreamingContext.getActiveOrCreate(checkpoint_dir,
                                             lambda: create_ssc(monitoring_dir,
                                                                result_dir,
                                                                max_micro_batches,
                                                                time_step_interval,
                                                                percentage_f
                                                                )
                                             )

    # 2. We start the Spark Streaming Context in the background to start receiving data.
    # Spark Streaming will start scheduling Spark jobs in a separate thread.

    # Very important: Please note a Streaming context can be started only once.
    # Moreover, it must be started only once we have fully specified what do we want it to do
    # when it receives data (i.e., the full set of transformations and ouptut operations we want it
    # to perform).
    ssc.start()

    # 3. As the jobs are done in a separate thread, to keep our application (this thread) from exiting,
    # we need to call awaitTermination to wait for the streaming computation to finish.
    ssc.awaitTerminationOrTimeout(time_step_interval)

    # 4. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir.
    streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose)

    # 5. Once we have transferred all files and processed them, we are done.
    # Thus, we stop the Spark Streaming Context
    ssc.stop(stopSparkContext=False)

    # 6. Extra security stop command: It acts directly over the Java Virtual Machine,
    # in case the Spark Streaming context was not fully stopped.

    # This is crucial to avoid a Spark application working on the background.
    # For example, Databricks, on its private version, charges per cluster nodes (virtual machines)
    # and hours of computation. If we, unintentionally, leave a Spark application working, we can
    # end up with an unexpected high bill.
    if (not sc._jvm.StreamingContext.getActive().isEmpty()):
        sc._jvm.StreamingContext.getActive().get().stop(False)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, makin th Pytho interprete to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input source folder (static dataset),
    # monitoring folder (dynamic dataset simulation) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    monitoring_dir = "/FileStore/tables/A02/my_monitoring/"
    checkpoint_dir = "/FileStore/tables/A02/my_checkpoint/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We specify the number of micro-batches (i.e., files) of our dataset.
    dataset_micro_batches = 16

    # 3. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 3

    # 4. We specify the maximum amount of micro-batches that we want to allow before considering data
    # old and dumping it.
    max_micro_batches = dataset_micro_batches + 1

    # 5. We configure verbosity during the program run
    verbose = False

    # 6. Extra input arguments
    percentage_f = 10

    # 7. We remove the monitoring and output directories
    dbutils.fs.rm(monitoring_dir, True)
    dbutils.fs.rm(result_dir, True)
    dbutils.fs.rm(checkpoint_dir, True)

    # 8. We re-create them again
    dbutils.fs.mkdirs(monitoring_dir)
    dbutils.fs.mkdirs(result_dir)
    dbutils.fs.mkdirs(checkpoint_dir)

    # 9. We call to my_main
    my_main(source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            percentage_f
            )