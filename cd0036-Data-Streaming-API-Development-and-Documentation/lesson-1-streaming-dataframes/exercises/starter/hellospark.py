from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/lesson-1-streaming-dataframes/exercises/starter/Test.txt
logFile = "/home/workspace/lesson-1-streaming-dataframes/exercises/starter/Test.txt"
# TO-DO: create a Spark session
spark = SparkSession.builder.appName("HelloSpark").getOrCreate()
# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")
# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 
logData = spark.read.text(logFile).cache()
# TO-DO: create a global variable for number of times the letter a is found
# TO-DO: create a global variable for number of times the letter b is found
numAs = 0
numBs = 0
# TO-DO: create a function which accepts a row from a dataframe, which has a column called value
# in the function increment the a count variable for each occurrence of the letter a
# in the value column
def countA(row):
    global numAs 
    numAs += row.value.count('a')
    print('***Total A Count', numAs)
# TO-DO: create another function which accepts a row from a dataframe, which has a column called value
# in the function increment the b count variable for each occurrence of the letter b
# in the value column
def countB(row):
    global numBs 
    numBs += row.value.count('b')
    print('***Total B Count', numBs)

# TO-DO: use the forEach method to invoke the a counting method
# TO-DO: use the forEach method to invoke the b counting method
logData.foreach(countA)
logData.foreach(countB)
# TO-DO: stop the spark application
spark.stop()