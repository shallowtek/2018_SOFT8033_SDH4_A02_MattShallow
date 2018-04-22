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
from __future__ import division
accum = sc.accumulator(0)

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
# FUNCTION get_averages - I thought he wanted average per cuisine so ignore function
# ------------------------------------------
# def get_averages(x, total_reviews):
  
#   cuisine = x[0]
#   cuisine_reviews = x[1][0]
#   numNegReviews = x[1][1]
#   points = x[1][2]
#   average = float(float(total_reviews)/float(cuisine_reviews))
  
#   return (cuisine, (cuisine_reviews, numNegReviews, points, round(average,1)))
 
# ------------------------------------------
# FUNCTION my_remove
# ------------------------------------------
def my_remove(x, percentage_f, average_reviews):

  reviews = x[1][0]
  numNegReviews = x[1][1]
  percentage_bad_reviews = (numNegReviews/average_reviews) * 100
  #rounded_p = round(percentage_bad_reviews, 1)
  
  if reviews >= average_reviews and percentage_bad_reviews < float(percentage_f):    
    return True
  else:
    return False
  
# ------------------------------------------
# FUNCTION my_remove
# ------------------------------------------
def my_sort(x):
  
  cuisine = x[0]
  reviews = x[1][0]
  numNegReviews = x[1][1]
  points = points = x[1][2]  
  average_points_per_view = points/reviews
  
  return (cuisine, (reviews, numNegReviews, points, average_points_per_view ))
  
  
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
  
  #Read each line
  inputRDD = sc.textFile(dataset_dir) 

   #Convert line to string and map to dictionary
  dictionaryRDD = inputRDD.map(lambda x: json.loads(x))
  
  #Split into key words
  splitRDD = dictionaryRDD.map(lambda x: my_split(x))
  
  # before (cuisine,(points, evaluation))
  
  #Get into a format that can be reduced by key (cuisine, (numReviews, numNegReviews, points))
  mapRDD = splitRDD.map(lambda x: my_reduce(x))
  
  #Old version reduce by key
  #filterRDD = splitRDD.reduceByKey(lambda x, y: tuple(map(sum, zip(my_reduce(x), my_reduce(y))))).sortBy(lambda x: x[1][0], False)
  #.sortBy(lambda x: x[1][0], False)
  #new working version reduce by key in correct format
  filterRDD = mapRDD.reduceByKey(lambda x, y: tuple(map(sum, zip(x,y)))).sortBy(lambda x: x[1][0], False)
  
  # after (cuisine,(numReviews, numNegReviews, points))
  
  #Get total reviews from accum1 cause less taxing
  total_reviews = accum.value
  #Get total reviews count, more costly but only way I could get all cuisines.
  total_cuisines = filterRDD.count()
  #Get average reviews for all cuisines  
  average_reviews = total_reviews / total_cuisines
  
#   print(total_reviews)
#   print(total_cuisines)
#   print(average_reviews)
  #Find average views for all cuisines
  #averageRDD = filterRDD.map(lambda x: get_averages(x, total_reviews))
  #averageRDD.persist()
  
  removeRDD = filterRDD.filter(lambda x: my_remove(x, percentage_f, average_reviews))
  
  sortRDD = removeRDD.map(lambda x: my_sort(x)).sortBy(lambda x: x[1][3], False)
  
  #Save results to text files
  #sortedRDD.saveAsTextFile(result_dir)
  
  #res = filterRDD
  
  for item in sortRDD.take(10):
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
    
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

   
    percentage_f = 22

    
    dbutils.fs.rm(result_dir, True)

    
    my_main(source_dir, result_dir, percentage_f)

