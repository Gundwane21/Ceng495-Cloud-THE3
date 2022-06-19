# Ceng495-Cloud-THE3

I use tsv as input instead of csv. To convert csv to tsv

# CONVERT

```
python convert.py < songs_normalize.csv > songs_normalize.tsv
or
python3 convert.py < songs_normalize.csv > songs_normalize.tsv
```
compiling is as same as stated in pdf

# COMPILE

```
hadoop com.sun.tools.javac.Main ∗.java
jar cf Hw3. jar ∗. class
```
Running is a little different. 

Firstly, use songs_normalize.tsv generated from above instead of songs_normalize.csv

Secondly, remove Hw3 from the run command stated in the pdf

# RUN

```
	hadoop jar Hw3.jar Total <tsv file path in hfs> output_total/
        hadoop jar Hw3.jar Average <tsv file path in hfs> output_average/
        hadoop jar Hw3.jar Popular <tsv file path in hfs> output_popular/
        hadoop jar Hw3.jar ExplicitlyPopular <tsv file path in hfs> output_explicitlypopular/
        hadoop jar Hw3.jar DanceByYear <tsv file path in hfs> output_dancebyyear/

```


