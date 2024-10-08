from mergeSchemaClass import *
from pyspark.sql.functions import col, when
from mergeHelpers import flattenhelper
import sparkUtils

spark= sparkUtils.getSparkSession()

# Set dynamic partitions to overwrite only the partition in DF
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
sc = spark.sparkContext
sc.setLogLevel("WARN")
##################################
# Arguments
# sys.argv[1] = file_format(parquet or avro)
# sys.argv[2] = entity_base_name(test_data)
# sys.argv[3] = Mode: I = Incremental (read only new or modified partitions); F = Full (read all partitions).
##################################


##################################
# Run Merge Schema
##################################
# Set up parameters
file_format = "parquet" #sys.argv[1]
entity = "{entity_base_name}_{file_format}".format(file_format=file_format, entity_base_name="test_data") #sys.argv[2]
data_path = "C:/Users/kunda/PycharmProjects/SparkSchemaMerge/spark_rawdata/{}".format(entity)
ctrl_file = "C:/Users/kunda/PycharmProjects/SparkSchemaMerge/spark_mergedata/last_read_control/{}.json".format(entity)
mode = "Full"#sys.argv[3]

merge_schema = MergeSchemaClass(spark, sc)


# Read partitions and merge schemas
print("\n**********************************************************")
print("Merging Schemas...".format(file_format))
print("Entity:", entity)
print("Format:", file_format)
print("**********************************************************")

rdd_json = merge_schema.merge_schemas(data_path, file_format, mode, ctrl_file)

if rdd_json != None:
    df = spark.read.json(rdd_json)
    # Check schema
    print("\nSchema after merging files:")
    df.printSchema()
    
    
    ##################################
    # Save data with new schema
    ##################################
    data_path_dest = 'C:/Users/kunda/PycharmProjects/SparkSchemaMerge/spark_mergedata/raw_schema_merged/{}'.format(entity)
    print("\nSaving merged files in {}".format(data_path_dest))
    df.write.partitionBy('date').mode('overwrite').format(file_format).save(data_path_dest)

    ##################################
    # Test data after schema merge
    ##################################
    # Read merged data
    df_merged = spark.read.option("mergeSchema", "true").format(file_format).load(data_path_dest)

    # Flatten DF
    df_merged_flat = df_merged.selectExpr(flattenhelper(df_merged.schema))

    # Count nulls by partition and column
    print("\nCount nulls by partition and column:")
    df_merged_flat.select(
        ["date"] + 
        [when(col(c).isNull(), 1).otherwise(0).alias(c) for c in df_merged_flat.columns if c != "date"]
    ).groupBy("date").sum().sort("date").show()

    # Count by partition
    print("\nCount by partition:")
    df_merged_flat.select(col("date")).groupBy("date").count().sort("date").show()
else:
    print("No new files to process")
    

