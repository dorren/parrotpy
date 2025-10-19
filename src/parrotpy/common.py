from faker import Faker
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, BooleanType, DateType, DoubleType, LongType, StringType

@udf(returnType=StringType())
def name():
    faker = Faker()
    return faker.name()

@udf(returnType=StringType())
def address():
    faker = Faker()
    return faker.address()

__all__ = ['name', 'address']