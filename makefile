all:
	hadoop com.sun.tools.javac.Main *.java
	jar cf Hw3.jar *.class
total:
	hadoop jar Hw3.jar Total /user/melisuzun/hw3/input/songs_normalize.tsv output_total/
clean:
	rm *.class
	rm Hw3.jar
	hdfs dfs -rm -r /user/melisuzun/output_total