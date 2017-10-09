Spark Lite
==========

What it it?
-----------

This project offers a Type class that defines common operations on Spark Dataset[T].
There are implementations for Dataset, RDD, Vector.
This allows to seamlessly switch from one implementation to another without changing your functions. 

Usages
-------

  * Unit testing: Starting a spark context and running jobs is slow when the number of rows is low. 
Using the Vector implementation speeds up your unit tests by an order of magnitude.
It also allows you to debug your tests more easily.

  * Optimization:
Sometimes it is not worth starting a Spark job when you know that the number of rows to process is low and can fit in one node.
You could add some heuristics to your program to run a computation on only one node
when the number of rows is lower than a certain threshold.

Dependencies
------------
The project depends on Spark 2.0.2 and Scala 2.11

How to install
--------------
There is no release yet, just check out and copy in your project.

Examples
-------------

