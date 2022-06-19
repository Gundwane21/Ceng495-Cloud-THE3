all:
	hadoop com.sun.tools.javac.Main *.java
	jar cf Hw3.jar *.class
run:
	hadoop jar Hw3.jar Total /user/melisuzun/hw3/input/songs_normalize.tsv output_total/
	hadoop jar Hw3.jar Average /user/melisuzun/hw3/input/songs_normalize.tsv output_average/
	hadoop jar Hw3.jar Popular /user/melisuzun/hw3/input/songs_normalize.tsv output_popular/
	hadoop jar Hw3.jar ExplicitlyPopular /user/melisuzun/hw3/input/songs_normalize.tsv output_explicitlypopular/
	hadoop jar Hw3.jar DanceByYear /user/melisuzun/hw3/input/songs_normalize.tsv output_dancebyyear/
total:
	hadoop jar Hw3.jar Total /user/melisuzun/hw3/input/songs_normalize.tsv output_total/
avg:
	hadoop jar Hw3.jar Average /user/melisuzun/hw3/input/songs_normalize.tsv output_average/
pop:
	hadoop jar Hw3.jar Popular /user/melisuzun/hw3/input/songs_normalize.tsv output_popular/
ex:
	hadoop jar Hw3.jar ExplicitlyPopular /user/melisuzun/hw3/input/songs_normalize.tsv output_explicitlypopular/
dc:
	hadoop jar Hw3.jar DanceByYear /user/melisuzun/hw3/input/songs_normalize.tsv output_dancebyyear/
out:
	hdfs dfs -cat /user/melisuzun/output_total/part-r-00000
	hdfs dfs -cat /user/melisuzun/output_average/part-r-00000
	hdfs dfs -cat /user/melisuzun/output_popular/part-r-00000
	hdfs dfs -cat /user/melisuzun/output_explicitlypopular/part-r-00000
	hdfs dfs -cat /user/melisuzun/output_dancebyyear/part-r-00000
clean:
	rm *.class
	rm Hw3.jar
	hdfs dfs -rm -r /user/melisuzun/output_total
	hdfs dfs -rm -r /user/melisuzun/output_average
	hdfs dfs -rm -r /user/melisuzun/output_popular
	hdfs dfs -rm -r /user/melisuzun/output_explicitlypopular
	hdfs dfs -rm -r /user/melisuzun/output_dancebyyear
