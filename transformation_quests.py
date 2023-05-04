import transformation as trans

import schemes as IMDB_SCHEMES

import settings as IMDB_PATHS

from pyspark.sql.functions import col, array_contains, trim, floor, row_number, explode, split
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType




def getTransform_1():
    # Получите все названия сериалов / фильмов и т. Д. которые доступны на украинском языке.
    akas = trans.getDataFromFile(IMDB_PATHS.TITLE_AKAS, IMDB_SCHEMES.schemaTitleAkas)
    return akas.select("title").where("region == 'UA'")




def getTransform_2():
    # Получите список имен людей, которые родились в 19 веке..
    names = trans.getDataFromFile(IMDB_PATHS.NAME_BASICS, IMDB_SCHEMES.schemaNameBasics)
    # век с 1 января 1801 года по 31 декабря 1900 года. 
    return names.select("primaryName").filter((col("birthYear") > 1800) & (col("birthYear") < 1901))




def getTransform_3():
    # Получите названия всех фильмов , которые длятся более 2 часов.
    akas = trans.getDataFromFile(IMDB_PATHS.TITLE_BASICS, IMDB_SCHEMES.schemaTitleBasics)
    return akas.select("primaryTitle", "originalTitle").filter(col("runtimeMinutes") > 120)




def getTransform_4():
    # Получите имена людей, соответствующих фильмов / сериалов и персонажей, которых они играли в этих фильмах.
    names = trans.getDataFromFile(IMDB_PATHS.NAME_BASICS, IMDB_SCHEMES.schemaNameBasics)
    principals = trans.getDataFromFile(IMDB_PATHS.TITLE_PRINCIPALS, IMDB_SCHEMES.schemaTitlePrincipals)
    titles = trans.getDataFromFile(IMDB_PATHS.TITLE_BASICS, IMDB_SCHEMES.schemaTitleBasics)
    return names.join(principals).filter(names["nconst"] == principals["nconst"]).join(titles).filter(titles["tconst"] == principals["tconst"]).select("primaryName", "characters", "primaryTitle")




def getTransform_5():
    # 5. Получите информацию о том, сколько фильмов/сериалов для взрослых и т. д. есть в каждом регионе. 
    # Получите 100 лучших из них из региона с наибольшим количеством в регион с наименьшим.
    akas = trans.getDataFromFile(IMDB_PATHS.TITLE_AKAS, IMDB_SCHEMES.schemaTitleAkas)
    basics = trans.getDataFromFile(IMDB_PATHS.TITLE_BASICS, IMDB_SCHEMES.schemaTitleBasics).filter(col("isAdult") == 1)
    return basics.join(akas).filter(basics["tconst"] == akas["titleId"]).groupby(col("region")).count().orderBy('count', ascending=False).limit(100)


def getTransform_6():
    # 6. Получите информацию о том, сколько эпизодов в каждом сериале. Получите 50 лучших из них, начиная с сериала с наибольшим количеством эпизодов.
    episodes = trans.getDataFromFile(IMDB_PATHS.TITLE_EPISODE, IMDB_SCHEMES.schemaTitleEpisode).groupBy(col("parentTconst")).count().orderBy('count', ascending=False).limit(50)
    basics = trans.getDataFromFile(IMDB_PATHS.TITLE_BASICS, IMDB_SCHEMES.schemaTitleBasics)
    return episodes.join(basics).filter(episodes["parentTconst"] == basics["tconst"]).select(basics["originalTitle"], 'count').orderBy('count', ascending=False)




def getTransform_7():
    # 7. Получите 10 наименований самых популярных фильмов/сериалов и т. д. за каждое десятилетие.
    basics = trans.getDataFromFile(IMDB_PATHS.TITLE_BASICS, IMDB_SCHEMES.schemaTitleBasics)
    ratings = trans.getDataFromFile(IMDB_PATHS.TITLE_RATINGS, IMDB_SCHEMES.schemaTitleRatings)
    
    result = basics.join(ratings).filter(basics["tconst"] == ratings["tconst"]).withColumn('decennary',  floor(col('startYear') % 10)).select('originalTitle', 'startYear', 'averageRating').filter((col("decennary") == 0))
    window = Window.partitionBy('startYear').orderBy(result['averageRating'].desc())
    return result.withColumn("row", row_number().over(window)).filter(col('row') <= 10)
    

    
    
def getTransform_8():
    # 8. Получите 10 наименований самых популярных фильмов/сериалов и т. д. в каждом жанре.
    basics = trans.getDataFromFile(IMDB_PATHS.TITLE_BASICS, IMDB_SCHEMES.schemaTitleBasics)
    ratings = trans.getDataFromFile(IMDB_PATHS.TITLE_RATINGS, IMDB_SCHEMES.schemaTitleRatings)
    result =  basics.join(ratings).filter(basics["tconst"] == ratings["tconst"]).select('originalTitle', 'averageRating','genres').withColumn('splgenres', explode(split('genres', ',')))
    window = Window.partitionBy('splgenres').orderBy(result['averageRating'].desc())
    return result.withColumn("row", row_number().over(window)).filter(col('row') <= 10).select("originalTitle", "averageRating", "splgenres", "row")