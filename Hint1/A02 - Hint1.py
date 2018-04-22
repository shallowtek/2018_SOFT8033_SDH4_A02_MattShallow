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

import json


# ------------------------------------------
# FUNCTION my_parse
# ------------------------------------------
def my_split(x):
  
  cuisine = x["cuisine"]
  
  evaluation = x["evaluation"]
  
  points = x["points"]
  
  return (cuisine, (points, evaluation))

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(x):

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
# FUNCTION sum_tuples
# ------------------------------------------
def sum_tuples(t1, t2):
  
  numReviews = t1[0] + t2[0]
  numNegReviews = t1[1] + t2[1]
  pounts = t1[2] + t2[2]
  
  return (numReviews, numNegReviews, points)
  
  
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
  
  #Read each line
  inputRDD = sc.textFile(dataset_dir) 

  #2. Convert line to string and map to dictionary
  dictionaryRDD = inputRDD.map(lambda x: json.loads(x))
  
  #Split into key words
  splitRDD = dictionaryRDD.map(lambda x: my_split(x))
  # before (cuisine,(points, evaluation))
  #map(operator.add, first,second) 
  #.sortBy(lambda x: x[1][0], False)
  mapRDD = splitRDD.map(lambda x: my_reduce(x))
  #filterRDD = splitRDD.reduceByKey(lambda x, y: tuple(map(sum, zip(my_reduce(x), my_reduce(y))))).sortBy(lambda x: x[1][0], False)
  filterRDD = mapRDD.reduceByKey(lambda x, y: tuple(map(sum, zip(x,y)))).sortBy(lambda x: x[1][0], False)
  
  
  
  # after (cuisine,(numReviews, numNegReviews, points))
  #9. Save results to text files
  #sortedRDD.saveAsTextFile(result_dir)
  
  #res = filterRDD
  
  for item in filterRDD.take(50):
    print(item)
 

  pass

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, makin th Pytho interprete to trigge
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input folder (dataset) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We add any extra variable we want to use
    percentage_f = 10

    # 3. We remove the monitoring and output directories
    dbutils.fs.rm(result_dir, True)

    # 5. We call to our main function
    my_main(source_dir, result_dir, percentage_f)

