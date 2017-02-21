all:	wordStatistics

wordStatistics:	wordStatistics.c
	gcc -pthread -o wordStatistics wordStatistics.c

clean: 
	rm -rf wordStatistics
