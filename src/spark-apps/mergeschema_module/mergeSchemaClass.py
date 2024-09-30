import datetime, os, time, json
from stat import S_ISREG, ST_CTIME, ST_MODE
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, StringType
from MergeSchemaHelper import *

class MergeSchemaClass:
    def __init__(self, spark, sc):
        self.spark = spark
        self.sc = sc



    """Input:
    - path1: Complete Parquet file path
    - path2: Complete Parquet file path
    - file_format: File format (parquet or avro)
    Output: Bool
    Returns true if path1 and path2 have the same schema.
    """
    def is_schema_equal(self, path1, path2, file_format):
        df1 = self.spark.read.format(file_format).load(path1).limit(0)
        df2 = self.spark.read.format(file_format).load(path2).limit(0)
        return df1.schema == df2.schema





    """ Input:
    - path: Entity path containing partitions
    - file_format: File format (parquet or avro)
    Output: Dict
    Returns a dict where each key represents a different schema with the initial path and final
    path containing that schema version, delimiting the boundaries of that version.
    """
    def identify_schema_changes(self, path, file_format, partition_list = []):
        dict = {}
        idx = 0
        for dir in sorted([d for d in os.listdir(path) if d.find("=") != -1]) if partition_list == [] else partition_list:
            # Add new key if not exist in dict
            if idx not in dict:
                dict[idx] = {"init_path": path + "/" + dir, "final_path": path + "/" + dir}
            # When schema is different, add new key to dict
            elif not self.is_schema_equal(dict[idx]["init_path"], path + "/" + dir, file_format):
                idx = idx + 1
                dict[idx] = {"init_path": path + "/" + dir, "final_path": path + "/" + dir}
            # When schema is equal, update final_path
            else:
                dict[idx]["final_path"] = path + "/" + dir
        return dict



    """Input:
    - dir_path: Entity path containing partitions.
    - file_format: File format to read (parquet or avro)
    - mode: "I" for incremental or "F" for full
    - ctrl_file: Complete file path of the file used to control the last time the path was read.
    Output: JSON RDD
    If mode = I returns a JSON RDD containing the union of all partitions modified since the timestamp in control partition read.
    If mode = F returns a JSON RDD containing the union of all partitions.
    All columns are converted to String in the returning RDD.
    """
    def merge_schemas(self, dir_path, file_format, mode, ctrl_file):
        merge_schema_helper = MergeSchemaHelper()

        idx = 0
        last_modified_ts_ctrl = 0 if mode == "F" else merge_schema_helper.get_last_read_ts(ctrl_file)

        # Check if there are new files to process
        if merge_schema_helper.get_modified_partitions(dir_path, last_modified_ts_ctrl) != []:
            lst_dir = [dir for dir, last_mod_ts in merge_schema_helper.get_modified_partitions(dir_path, last_modified_ts_ctrl)]
            try:
                if len(self.identify_schema_changes(dir_path, file_format, lst_dir).keys()) > 1:
                    print("Different schemas identified:")
                    print(json.dumps(self.identify_schema_changes(dir_path, file_format, lst_dir), indent = 4))
                print("\nProcessing files:")
                # Read each directory and create a JSON RDD making a union of all directories
                for dir in lst_dir:
                    print("idx: " + str(idx) + " | path: " + dir_path + "/" + dir)
                    # Get schema
                    schema = self.spark.read.format(file_format).load(dir_path + "/" + dir).limit(0).schema

                    # Read file converting to string
                    df_temp = (self.spark.read
                                .format(file_format)
                                .load(dir_path + "/" + dir)
                                .selectExpr(merge_schema_helper.convert_columns_to_string(schema))
                                .withColumn(dir.split("=")[0], lit(dir.split("=")[1]))
                            )
                    # Convert to JSON to avoid error when union different schemas
                    if idx == 0:
                        rdd_json = df_temp.toJSON()
                    else:
                        rdd_json = rdd_json.union(df_temp.toJSON())
                    idx = idx + 1
                # Set Timestamp in control file
                merge_schema_helper.set_last_read_ts(ctrl_file)
                return rdd_json
            except Exception as e:
                print("Unexpected error: ", e)
        else:
            return None

