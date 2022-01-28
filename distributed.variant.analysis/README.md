# Distributed Variant Analysis


## Setup

**Copy the data file to the HDFS**

```
hadoop fs -mkdir -p /data/distributed_variant_analysis/input
hadoop fs -put data/input_run_AAAAA.csv /data/distributed_variant_analysis/input
hadoop fs -put data/input_run_BBBBB.csv /data/distributed_variant_analysis/input
hadoop fs -put data/input_run_CCCCC.csv /data/distributed_variant_analysis/input
hadoop fs -ls /data/distributed_variant_analysis/input
```

**Building the application**

Specify profile with the name of the main class you want ot use. 

```
mvn clean compile assembly:single -P SimpleVariantCounts0
```



**Running the application**

```
hadoop fs -rmdir /data/distributed_variant_analysis/output
hadoop jar target/distributed.variant.analysis-0.0.1-SNAPSHOT.jar \
	/data/distributed_variant_analysis/input \
	/data/distributed_variant_analysis/output
hadoop fs -ls /data/distributed_variant_analysis/output
hadoop fs -cat /data/distributed_variant_analysis/output/part-r-00000
```

## Input File(s)

Variant calls are specified in the CSV format: 

```
run_id, chromosome, position_start, position_end, reference_base, variant_base, db_snp_id, significance
```

Variants from multiple runs can be placed in a single file or divided into multiple files.   




## Sample Input

The ``data/`` directory contains three input files. Each input file contains five variant calls from a different run. 
Runs AAAAA and BBBBB have a common pathogenic (rs121913400) and benign (rs112326758) variant so that pair will be at the top of the list in the output. 


## To Do

- [ ] Handle special case of runs that don't have any benign (or pathogenic) variants. 
- [ ] Update pom.xml so you can specify which class to use as main in the jar manifest. 