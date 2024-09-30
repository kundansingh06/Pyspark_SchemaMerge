from pyspark.sql.types import StructType, ArrayType  


def flattenhelper(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += flattenhelper(dtype, prefix=name)
        else:
            fields.append(name + " AS " + name.replace(".","_"))
    return fields