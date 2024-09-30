import datetime
from dataGenerator import DataGenerator
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
# sys.argv[3] = num_rows(10)
##################################

##################################
# Generate test data
##################################

# Set up parameters
file_format = "parquet" #sys.argv[1]
entity = "{entity_base_name}_{file_format}".format(file_format=file_format, entity_base_name="test_data") #sys.argv[2])
data_path = "C:/Users/kunda/PycharmProjects/SparkSchemaMerge/spark_rawdata/{}".format(entity)
num_rows = int(10) #int(sys.argv[3])

# Generate test data
print("\n**********************************************************")
print("Generating test data...".format(file_format))
print("Entity:", entity)
print("Format:", file_format)
print("**********************************************************")
data_gen = DataGenerator(spark, sc)

data_gen.gen_data_simple_schema(data_path, datetime.date(2020,1,1), num_rows, file_format)

data_gen.gen_data_add_nested_struct(data_path, datetime.date(2020,2,1), num_rows, file_format)

data_gen.gen_data_add_columns(data_path, datetime.date(2020,3,1), num_rows, file_format)

data_gen.gen_data_change_datatype_add_struct(data_path, datetime.date(2020,4,1), num_rows, file_format)

data_gen.gen_data_change_column_name(data_path, datetime.date(2020,5,1), num_rows, file_format)

data_gen.gen_data_remove_column(data_path, datetime.date(2020,6,1), num_rows, file_format)

print("completed successfully")