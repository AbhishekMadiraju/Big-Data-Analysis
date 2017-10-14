Programming Assignment
----------------------

1. I have written two java Programs:

DominantStates.java for 2 (a) and StatesSameRanking for 2 (b).

2. Temporary output of first map is stored in /temp_data

3. The custom AMI name is linux-hadoop-madiraju. And the ami id is ami-bf8340c5.
It has hadoop and java setup along with common configurations required for all nodes. You will need to change dns and IP values wherever required and copy the core-site.xml file with appropriate changes across all nodes. If you run into any issues with this, please le me know.

Running
-------

The follwing commands need to be executed in the given order:

hadoop com.sun.tools.javac.Main FileName.java

jar cf FileName.jar FileName*.class

hadoop jar FileName.jar FileName /states /output

Output
------

To view the output on command line:

hadoop fs -cat /path_to_file/filename

Issues
------

The JVM didnt get enoguh memory after executing the program many times. I rebooted the instance after which I was able to run both programs.

Change