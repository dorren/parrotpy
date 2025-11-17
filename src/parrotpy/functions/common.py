from faker import Faker
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def person_name():
    faker = Faker()
    return faker.name()

@udf(returnType=StringType())
def address():
    faker = Faker()
    return faker.address()

__all__ = ['person_name', 'address']