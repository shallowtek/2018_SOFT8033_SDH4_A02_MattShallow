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
  
  return (cuisine,(points,evaluation))

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(x):
  
  points = 0
  numReviews = 0
  numNegReviews = 0
  numReviews = numReviews + 1
  
  if x[1] == "Negative":
    numNegReviews = numReviews + 1
    points -= x[0]
  else:
    points = points + x[0]
    
  
  return (numReviews,numNegReviews, points)

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
  #tuple(map(sum,zip(a,b)))
  filterRDD = splitRDD.reduceByKey(lambda x, y: tuple(map(sum, zip(my_reduce(x),my_reduce(y))))).sortBy(lambda x: x[1][0], False)
 
  #9. Save results to text files
  #sortedRDD.saveAsTextFile(result_dir)
  
  #res = filterRDD
  
  for item in filterRDD.take(20):
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

