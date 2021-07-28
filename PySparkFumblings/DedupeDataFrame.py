from pyspark.sql import SparkSession
from pyspark.sql import Row

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import types
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T

# How do I deduplicate a pyspark dataframe column when distinct doesn't cut it?
# If you're from a database world, and your databases are case insensitive, then
# this is a recipe for you.
#
# Assume two rows with values of TOYS and Toys. Calling distinct on that will yield
# two rows because of case sensitivity.
#
# Another scenario is trailing whitespace (or leading). "TOYS" and "TOYS " are 
# not the same.
#
# The final scenario we'll examine is internationlization. Back in the early
# 2000s, we used the Unicode Hammer https://code.activestate.com/recipes/251871-latin1-to-ascii-the-unicode-hammer/
# but python now has a native casefold method to handle some of what we used the
# hammer for.


def generate_match_key(src_col):
    """Convert a source string into the internationalize lower cased versio of itself with
    whitespace removed
    TODO: Look up python document strings 
    """
    return src_col.casefold().strip()

# Create a function pointer for use in pyspark 
match_key_udf = F.UserDefinedFunction(generate_match_key, T.StringType())

debug_point = 0
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()




_columns = ['ProductCode', 'ProductName']
# Eszett can be expressed as either ß or ss
_vals = [('AAA', 'A big thing'), ('AAA', 'a big thing'), ('BBB', 'TOYS'), ('BBB', 'TOYS '), ('CCC', 'Normal'), ('DE', 'Straße'), ('DE', 'Strasse')]
_df = spark.createDataFrame(_vals, _columns)

_df = _df.withColumn("ProductMatchName", match_key_udf(_df['ProductName'])).alias('O')
# Segment the data by creating a row number within the match name giving preference to the first product name (favors Upper case)
_deduped = _df.withColumn("rn", row_number().over(Window.partitionBy("ProductMatchName").orderBy(asc("ProductName"))))

# Remove any rows that were not the primary
_deduped = _deduped.filter(col("rn") == 1).drop("rn").alias('D')

print("Showing _deduped")

# I should only have 3 rows
_deduped.show()

#+-----------+-----------+----------------+
#|ProductCode|ProductName|ProductMatchName|
#+-----------+-----------+----------------+
#|        CCC|     Normal|          normal|
#|         DE|    Strasse|         strasse|
#|        AAA|A big thing|     a big thing|
#|        BBB|       TOYS|            toys|
#+-----------+-----------+----------------+


# now, how do I update my original data frame to only have the de-deuplicated match name?

# Preserve our starting point
_df_original = _df

# Join based on ProductMatchName
_df_full = _df.join(_deduped, on = _df.ProductMatchName == _deduped.ProductMatchName, how = "inner")#.select(_df.ProductCode, _deduped.ProductName)

print("Showing _df_full")
_df_full.show()

#|ProductCode|ProductName|ProductMatchName|ProductCode|ProductName|ProductMatchName|
#+-----------+-----------+----------------+-----------+-----------+----------------+
#|        CCC|     Normal|          normal|        CCC|     Normal|          normal|
#|         DE|     Straße|         strasse|         DE|    Strasse|         strasse|
#|         DE|    Strasse|         strasse|         DE|    Strasse|         strasse|
#|        AAA|A big thing|     a big thing|        AAA|A big thing|     a big thing|
#|        AAA|a big thing|     a big thing|        AAA|A big thing|     a big thing|
#|        BBB|       TOYS|            toys|        BBB|       TOYS|            toys|
#|        BBB|      TOYS |            toys|        BBB|       TOYS|            toys|
#+-----------+-----------+----------------+-----------+-----------+----------------+

# Now, wot in tarnation is going on here
# I do love helpful error messages
# Column ProductCode#0, ProductName#1, ProductName#1 are ambiguous. It's probably because you joined several Datasets together, and some of these Datasets are the same. This column points to one of the Datasets but Spark is unable to figure out which one. Please alias the Datasets with different names via `Dataset.as` before joining them, and specify the column using qualified name, e.g. `df.as("a").join(df.as("b"), $"a.id" > $"b.id")`. You can also set spark.sql.analyzer.failAmbiguousSelfJoin to false to disable this check.
#_df_does_not_work = _df.join(_deduped, on = _df.ProductMatchName == _deduped.ProductMatchName, how = "inner").select(_df.ProductCode, _deduped.ProductName, _df.ProductName)

# By using join columns in an array, they are not duplicated on output
# 5 columns below, not 6
_df_wot = _df.join(_deduped, on = ['ProductMatchName'], how = "inner")#.select(_df.ProductCode, _deduped.ProductName, _df.ProductName)
_df_wot.show()

# Or this error if we try to SELECT it
# cannot resolve '`O.ProductCode, D.ProductName`' given input columns: [O.ProductCode, D.ProductCode, O.ProductMatchName, D.ProductMatchName, O.ProductName, D.ProductName];
#'Project ['O.ProductCode, D.ProductName]
#_df_fin = _df.join(_deduped, on = _df.ProductMatchName == _deduped.ProductMatchName, how = "inner").select('O.ProductCode, D.ProductName')
# The above error is that I needed to identify each column separately - see below

# By aliasing and 
_df_fin = _df.join(_deduped, on = _df.ProductMatchName == _deduped.ProductMatchName, how = "inner").select('O.ProductCode', 'D.ProductName')


_df_fin.show()

#+-----------+-----------+
#|ProductCode|ProductName|
#+-----------+-----------+
#|        CCC|     Normal|
#|         DE|    Strasse|
#|         DE|    Strasse|
#|        AAA|A big thing|
#|        AAA|A big thing|
#|        BBB|       TOYS|
#|        BBB|       TOYS|
#+-----------+-----------+