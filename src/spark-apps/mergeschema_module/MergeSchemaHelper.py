import datetime, os, time, json
from stat import S_ISREG, ST_CTIME, ST_MODE
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, StringType


class MergeSchemaHelper:


    """Input:
    - dir_path: Entity path containing partitions
    - ts_filter: Timestamp to filter the partitions returned (filter by last modification)
    Output: List
    Returns a list of partition directories that were modified after the ts_filter provided as parameter.
    """
    # All directories inside the dir_path that matches partition pattern
    def get_modified_partitions(self, dir_path, ts_filter):
        lst_dir = [d for d in os.listdir(dir_path) if d.find("=") != -1]
        if lst_dir != []:
            # Filter based on modification date
            return sorted([(dir, round(os.stat(dir_path + "/" + dir).st_mtime * 1000)) for dir in lst_dir if round(os.stat(dir_path + "/" + dir).st_mtime * 1000) > ts_filter])
        else:
            return []



        """Input:- file_path: Complete file path of the file used to control the partition read.
        Output: Integer
        Returns the timestamp of the last time that a partition was read.
        """
    def get_last_read_ts(self, file_path):
        if os.path.exists(file_path):
            # Read control file
            with open(file_path) as inputfile:
                ctrl_file = json.load(inputfile)
                last_modified_ts_ctrl = ctrl_file["last_read_ts"]
        else:
            last_modified_ts_ctrl = 0
        return last_modified_ts_ctrl



    """
    Input:
    - file_path: Complete file path of the file used to control the partition read.
    Output: None
    Creates or overwrite the file used to control the partition read.
    """
    # Get only the path without file
    def set_last_read_ts(self, file_path):
        path = "/".join(file_path.split("/")[:-1])
        # Check if path exist. If not, create
        if not os.path.exists(path):
            os.makedirs(path)
        # Save ts
        with open(file_path, "w+") as outfile:
            json.dump({"last_read_ts": round(time.time() * 1000)}, outfile)
        return



    """Input:
    - schema: Dataframe schema as StructType
    Output: List
    Returns a list of columns in the schema casting them to String to use in a selectExpr Spark function.
    """
    def convert_columns_to_string(self, schema, parent="", lvl=0):
        lst = []
        for x in schema:
            if lvl > 0 and len(parent.split(".")) > 0:
                parent = ".".join(parent.split(".")[0:lvl])
            else:
                parent = ""
            if isinstance(x.dataType, StructType):
                parent = parent + "." + x.name
                nested_casts = ",".join(self.convert_columns_to_string(x.dataType, parent.strip("."), lvl=lvl + 1))
                lst.append("struct({nested_casts}) as {col}".format(nested_casts=nested_casts, col=x.name))
            else:
                if parent == "":
                    lst.append("cast({col} as string) as {col}".format(col=x.name))
                else:
                    lst.append("cast({parent}.{col} as string) as {col}".format(col=x.name, parent=parent))
        return lst


