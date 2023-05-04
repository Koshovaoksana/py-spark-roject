
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType

schemaNameBasics = StructType([
    StructField("nconst", StringType(),True),
    StructField("primaryName", StringType(),True),
    StructField("birthYear", IntegerType(),True),
    StructField("deathYear", IntegerType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True)
  ])

schemaTitleAkas = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True),
    StructField("attributes", StringType(), True),
    StructField("isOriginalTitle", BooleanType(), True)
  ])

schemaTitleBasics = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", IntegerType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", StringType(), True)
  ])

schemaTitleCrew = StructType([
    StructField("tconst", StringType(),True),
    StructField("directors", StringType(),True),
    StructField("writers", StringType(),True)
  ])

schemaTitleEpisode = StructType([
    StructField("tconst", StringType(), True),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", IntegerType(),True),
    StructField("episodeNumber", IntegerType(),True)
  ])

schemaTitlePrincipals = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True)
  ])

schemaTitleRatings = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", StringType(), True),
    StructField("numVotes",StringType(), True)
  ])

