# Producer-Consumer-using-Pthreads

Producer-Consumer problem has been implemented by a multi-threaded program using Pthreads library.
The threads read a file of words and generates some statistics such as the count of each word
and the count of words that start with the same letter. 
 The solution consists of a single process, which divides the workamong multiple threads.

The producer consumer problem has been implemented using pthreads and the file is named "wordStatistics.c".
The synchronization between the threads was achieved using mutex and conditional variables.
The function/task and the number of threads associated with it are as follows:

Mapper pool updater: 1 thread
Mapper             : user defined
Reducer            : user defined
Word count writer  : 1 thread

Only 2 inputs (number of mapper threads and number of reducer threads) along with input file are required.

The makefile is included.
