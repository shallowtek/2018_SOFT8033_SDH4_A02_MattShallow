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
# FUNCTION split
# ------------------------------------------
#split function checks the dictionary that is passed in for key words and returns a tuple
def split(x):
	
	cuisine = x["cuisine"]
  
	evaluation = x["evaluation"]
  
	points = x["points"]
   
  
	return (cuisine,(points,evaluation))
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):

	inputRDD = sc.textFile(dataset_dir)
  
    #df = sqlContext.read.json(dataset_dir)
    #df.sort_values(by='cuisine')
	
	#json.loads() decodes the json to a dictionary
	dictionaryRDD = inputRDD.map(lambda x: json.loads(x))
	#dataFrame.show() 
	
	#split the dictionary into cuisine, points and evaluation and then group by cuisine
	splitLineRDD = dictionaryRDD.map(lambda x: split(x)).groupBy(lambda y: y[0])
	
	for item in splitLineRDD.take(5):
		print(item)
  
    pass

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
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

    # 4. We call to our main function
    my_main(source_dir, result_dir, percentage_f)
