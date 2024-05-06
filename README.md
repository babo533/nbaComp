part-00000-1126c397-bc9b-435a-9de9-555548b6bef6-c000.csv - This is where the CSV files are saved for the spark database

dsci.py is where I initially loaded in the CSV into MongoDB

main.py is where the backend is linked to the frontend, with our tech stack being streamlit for the frontend and mongoDB/Spark holding our databases with python being our primary language, this is where most of the functionsand filtering is done.

spark.py is where we initialized the spark database and added our initial CSV file into it


To get the code to be fully functional, just run: streamlit run /Users/hoon/Desktop/streamLit/main.py 
in the terminal
