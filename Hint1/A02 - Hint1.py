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
accum = sc.accumulator(0)
accum2 = sc.accumulator(0)
# ------------------------------------------
# FUNCTION my_split
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
  accum.add(1)
  
  if my_tuple[1] == "Negative":
    numNegReviews = 1
    points -= my_tuple[0]
  else:
    points = points + my_tuple[0]
    
  
  
  return (cuisine,(numReviews, numNegReviews, points))
# ------------------------------------------
# FUNCTION get_averages
# ------------------------------------------
def get_averages(x, total_reviews):
  
  cuisine = x[0]
  cuisine_reviews = x[1][0]
  numNegReviews = x[1][1]
  points = x[1][2]
  average = float(float(total_reviews)/float(cuisine_reviews))
  
  return (cuisine, (cuisine_reviews, numNegReviews, points, round(average,1)))
 
# ------------------------------------------
# FUNCTION my_remove
# ------------------------------------------
# def my_remove(x, percentage_f):
#   cuisine = x[0]
#   reviews = x[1][0]
#   numNegReviews = x[1][1]
#   points = x[1][2]
#   average = x[1][3]
#   
#   if reviews >= average
    
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
  
  #Get into a format that can be reduced by key (cuisine, (numReviews, numNegReviews, points))
  mapRDD = splitRDD.map(lambda x: my_reduce(x))
  
  #Old version reduce by key
  #filterRDD = splitRDD.reduceByKey(lambda x, y: tuple(map(sum, zip(my_reduce(x), my_reduce(y))))).sortBy(lambda x: x[1][0], False)
  
  #new working version reduce by key in correct format
  filterRDD = mapRDD.reduceByKey(lambda x, y: tuple(map(sum, zip(x,y)))).sortBy(lambda x: x[1][0], False)
  
  # after (cuisine,(numReviews, numNegReviews, points))
  
  #Get total reviews from accum1
  total_reviews = splitRDD.count()
  #Get total reviews from accum2
  total_cuisines = filterRDD.count()
  #Get average reviews for all cuisines
  
  average_reviews = float(float(total_reviews) / float(total_cuisines))
  print(total_reviews)
  print(total_cuisines)
  print(average_reviews)
  #Find average views for all cuisines
  #averageRDD = filterRDD.map(lambda x: get_averages(x, total_reviews))
  #averageRDD.persist()
  
  #removeRDD = filterRDD.map(lambda x: my_remove(x, percentage_f, ))
  
 
  #9. Save results to text files
  #sortedRDD.saveAsTextFile(result_dir)
  
  #res = filterRDD
  
#   for item in splitRDD.take(30):
#     print(item)
 

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

