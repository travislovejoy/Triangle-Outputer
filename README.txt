Getting hands on experience with MapReduce using hadoop an opensource MapReduce application and then using whirr to run haddop on Amazon EC2 cloud computing services for larger data sets such as Email_Enron.txt, which can be found here: http://snap.stanford.edu/data/email-Enron.html

TriangleOutputer.java
A modification to TriangleCount.java found here: https://github.com/vertica/Graph-Analytics----Triangle-Counting
Taking an input in for of <node1> <whitespace> <node2> <endline> and outputing the nodes forming a triangle.

Ex.
Input: 
	1 2
	2 3

Output:
	1 2 3





Compiling and running code using hadoop

--$ javac -classpath *:lib/* -d triangleoutputer_classes TriangleOounter.java
--$ jar -cvf triangleoutputer.jar -C triangleoutputer_classes/ .
--$ bin/hadoop jar triangleoutputer.jar TriangleOutputer <input> <output>


