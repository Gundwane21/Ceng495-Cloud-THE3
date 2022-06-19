# TEST PROD PURPOSES
convert:
	python convert.py < songs_normalize.csv > songs_normalize.tsv

# AFTER converting to tsv, put tsv file into desired hadoop file system
# CHANGE the following path to tsv file path in HADOOP FILE SYSTEM 
HFS_TSV_PATH = /user/melisuzun/hw3/input/songs_normalize.tsv

all:
	hadoop com.sun.tools.javac.Main *.java
	jar cf Hw3.jar *.class
run:
	hadoop jar Hw3.jar Total ${HFS_TSV_PATH} output_total/
	hadoop jar Hw3.jar Average  ${HFS_TSV_PATH} output_average/
	hadoop jar Hw3.jar Popular ${HFS_TSV_PATH} output_popular/
	hadoop jar Hw3.jar ExplicitlyPopular ${HFS_TSV_PATH} output_explicitlypopular/
	hadoop jar Hw3.jar DanceByYear ${HFS_TSV_PATH} output_dancebyyear/


# DEBUG PURPOSES 
total:
	hadoop jar Hw3.jar Total  ${HFS_TSV_PATH} output_total/
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
