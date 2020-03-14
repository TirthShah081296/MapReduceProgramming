Readme

Map-Reduce program that will perform EquiJoin using Hadoop Framework 2.7.7 in Java 1.8.

The code takes in two inputs, first argument is the hdfs location of the input file upon which equijoin will be performed. Second argument is the hdfs location where the output file is stored.

Approach:

The code works on the data where we have to perform equijoin on two tables. I will work through the process explaination with example.

There are 3 parts of the code:
1. Mapper Class: This class contains the method map(). The method takes input line and then trims extra space. The string is comma seperable. The string is seperated and words are stored in as array_list.
The second element of the array is our join attribute. The input of the mapper is the line of string which has the key(join attribute) and value in it. The output of the mapper function is list of(key,values).

Example input:
order,1,aaa,d1
line,1,10,1
order,2,aaa,d2
line,1,20,3
order,3,bbb,d3
line,2,10,5
line,2,50,100
line,3,20,1

Here the key is 2nd attribute in the line. The output of the mapper function will be as follows:
[key=1, values = (order,1,aaa,d1)],[key=1, values = (line,1,10,1)],[key=2,values = (order,2,aaa,d2)],[key=1, values = (line,1,20,3)],
[key=3, values = (order,3,bbb,d3)],[key=2, values = (line,2,10,5)],[key=2, values = (line,2,50,100)],[key=3, values = (line,3,20,1)]

The list of <key,values> pair undergoes shuffling and passes to the reducer in the format (key,list(values))

2. Reducer Class: This class contains the method reduce(). This method takes input as <key,list(values)>. The method makes two vectors for list of values of partition A and partition B. Initially the first line first argument is consider as first partition name. I iterate over the next values to append the values to corresponding partition list (either A or B). I iterate over 2 loops to get values from partition A and partition B and combine them. I store the values in as set to avoid duplicate/redundant row. The output of the reduce function is list<key,values> combined together for both tables.

Example input:
key=1, values = [(order,1,aaa,d1),(line,1,10,1),(line,1,20,3)]
key=2, values = [(order,2,aaa,d2),(line,2,10,5),(line,2,50,100)]
key=3, values = [(order,3,bbb,d3),(line,3,20,1)]

output of the reducer:
line,1,20,3, order,1,aaa,d1
line,1,10,1, order,1,aaa,d1
line,2,50,100, order,2,aaa,d2
line,2,10,5, order,2,aaa,d2
line,3,20,1, order,3,bbb,d3

3. Driver Class(Main): In Main class, configuration object is created. This object is used to set the job. The equiJoinMapper class and equiJoinReducer class are set using this job. The outputkey and outputvalue class along with the inputfile and outputfile path are set with the job instance. The args[0] corresponds to input path and args[1] corresponds to the output path.

