from typing import List
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import count, spark_partition_id, lower, udf, broadcast, date_sub, current_date, current_timestamp, col, collect_list, floor, year, avg, rank, expr
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import time
from functools import wraps
import sys


# utility to measure the runtime of some function
def timeit(f):
    @wraps(f)
    def wrap(*args, **kw):
        t = time.time()
        result = f(*args, **kw)
        print(f'{f.__name__}: {round(time.time() - t, 3)}s')
        return result
    return wrap


# show the number of rows in each partition
def showPartitionSize(df: DataFrame):
    df = df.groupBy(spark_partition_id().alias('id')).count()
    df.show(df.rdd.getNumPartitions())

#query 1
# SELECT *
# FROM (
#     SELECT t.id,
#         left(t.primary_title, 30),
#         ((start_year / 10) * 10)::int AS decade,
#         avg(uh.rating) AS rating,
#         rank() over (
#             PARTITION by ((start_year / 10) * 10) :: int
#             ORDER BY avg(uh.rating) DESC, t.id
#         ) AS rank
#     FROM title t
#     JOIN userHistory uh ON uh.title_id = t.id
#     WHERE t.title_type = 'movie'
#         AND ((start_year / 10) * 10)::int >= 1980
#         AND t.id IN (
#             SELECT title_id
#             FROM titleGenre tg
#             JOIN genre g ON g.id = tg.genre_id
#             WHERE g.name IN (
#                 'Drama'
#             )
#         )
#         AND t.id IN (
            # SELECT title_id
            # FROM titleAkas
            # WHERE region IN (
            #     'US', 'GB', 'ES', 'DE', 'FR', 'PT'
            # )
#         )
#     GROUP BY t.id
#     HAVING count(uh.rating) >= 3
#     ORDER BY decade, rating DESC
# ) t_
# WHERE rank <= 10;

@timeit
def query1_dataframe(title: DataFrame,
                    userHistory: DataFrame,
                    titleGenre: DataFrame,
                    genre: DataFrame,
                    titleAkas: DataFrame
                    ) -> List[Row]:

    rank_window = Window.partitionBy(col("decade")).orderBy(col("rating").desc(), title.id)

    #title.join(titleAkas.region.isin(['US', 'GB', 'ES', 'DE', 'FR', 'PT']).select(titleAkas.title_id).distinct(), title.id == titleAkas.title_id).

    return title\
        .where(title.title_type == 'movie') \
        .where(title.start_year >= 1980) \
        .join(titleAkas.where(titleAkas.region.isin('US', 'GB', 'ES', 'DE', 'FR', 'PT')).select(titleAkas.title_id).distinct(), title.id == titleAkas.title_id)\
        .join(titleGenre.where(titleGenre.genre_id == 8).distinct(), titleGenre.title_id == title.id)\
        .join(userHistory, userHistory.title_id == title.id) \
        .groupBy(title.id, title.primary_title, (floor(title.start_year / 10) * 10).alias("decade")) \
        .agg(avg(userHistory.rating).alias("rating"),\
            count(userHistory.rating).alias('count')) \
        .where(col('count') >= 3) \
        .select(title.id, title.primary_title, col("decade"), col('rating'), rank().over(rank_window).alias("rank"))\
        .orderBy(col("decade"), col("rating").desc()) \
        .where(col("rank") <= 10)\
        .collect()

@timeit
def query2(spark: SparkSession) -> List[Row]:
    return spark.sql("""
SELECT t.id, t.primary_title, tg.genres, te.season_number, count(*) AS views
FROM title t
JOIN titleEpisode te ON te.parent_title_id = t.id
JOIN title t2 ON t2.id = te.title_id
JOIN userHistory uh ON uh.title_id = t2.id
JOIN users u ON u.id = uh.user_id
JOIN (
    SELECT tg.title_id, array_agg(g.name) AS genres
    FROM titleGenre tg
    JOIN genre g ON g.id = tg.genre_id
    GROUP BY tg.title_id
) tg ON tg.title_id = t.id
WHERE t.title_type = 'tvSeries'
    AND uh.last_seen BETWEEN NOW() - INTERVAL '30 days' AND NOW()
    AND te.season_number IS NOT NULL
    AND u.country_code NOT IN ('US', 'GB')
GROUP BY t.id, t.primary_title, tg.genres, te.season_number
ORDER BY count(*) DESC, t.id
LIMIT 100;
    """).collect()


#works
@timeit
def query2_dataframe(title: DataFrame, 
                    titleEpisode: DataFrame,
                    userHistory: DataFrame, 
                    users: DataFrame, 
                    titleGenre: DataFrame,
                    genre: DataFrame
                    ) -> List[Row]:
    tg = titleGenre.join(genre, genre.id == titleGenre.genre_id) \
        .groupBy(titleGenre.title_id) \
        .agg(collect_list(genre.name).alias('genres')) \
        .select(titleGenre.title_id, col("genres"))

    return title.alias("t")\
        .join(titleEpisode, titleEpisode.parent_title_id == col("t.id")) \
        .join(title.alias("t2"), titleEpisode.title_id == col("t2.id")) \
        .join(userHistory, userHistory.title_id == col("t2.id")) \
        .join(users, users.id == userHistory.user_id) \
        .join(tg, tg.title_id == col("t.id")) \
        .where(col("t.title_type") == "tvSeries") \
        .where(titleEpisode.season_number.isNotNull())\
        .where(~users.country_code.isin(["US", "GB"]))\
        .where(userHistory.last_seen.between(current_timestamp() - expr("INTERVAL 30 DAYS"),current_timestamp()))\
        .groupBy(col("t.id"), col("t.primary_title"), tg.genres, titleEpisode.season_number)\
        .agg(count("*").alias("views"))\
        .orderBy(col("views").desc(), col("t.id"))\
        .limit(100)\
        .select(col("t.id"), col("t.primary_title"), tg.genres, titleEpisode.season_number, col("views"))\
        .collect()

@timeit
def query3(spark: SparkSession) -> List[Row]:
    return spark.sql("""
SELECT n.id,
    n.primary_name,
    date_part('year', NOW())::int - n.birth_year AS age,
    count(*) AS roles
FROM name n
JOIN titlePrincipals tp ON tp.name_id = n.id
JOIN titlePrincipalsCharacters tpc ON tpc.title_id = tp.title_id
    AND tpc.name_id = tp.name_id
JOIN category c ON c.id = tp.category_id
JOIN title t ON t.id = tp.title_id
LEFT JOIN titleEpisode te ON te.title_id = tp.title_id
WHERE t.start_year >= date_part('year', NOW())::int - 10
    AND c.name = 'actress'
    AND n.death_year IS NULL
    AND t.title_type IN (
        'movie', 'tvSeries', 'tvMiniSeries', 'tvMovie'
    )
    AND te.title_id IS NULL
GROUP BY n.id
ORDER BY roles DESC
LIMIT 100;
""").collect()

#works
def query3_dataframe(name: DataFrame,
                    titlePrincipals: DataFrame,
                    titlePrincipalsCharacters: DataFrame,
                    category: DataFrame,
                    title: DataFrame,
                    titleEpisode: DataFrame
                    ) -> List[Row]:
    return name.join(titlePrincipals, name.id == titlePrincipals.name_id) \
        .join(titlePrincipalsCharacters, (titlePrincipalsCharacters.title_id == titlePrincipals.title_id) & (titlePrincipalsCharacters.name_id == titlePrincipals.name_id)) \
        .join(category, category.id == titlePrincipals.category_id) \
        .join(title, title.id == titlePrincipals.title_id) \
        .join(titleEpisode, titleEpisode.title_id == titlePrincipals.title_id, how='left') \
        .where(title.start_year >= year(current_date()) - 10) \
        .where(category.name == 'actress') \
        .where(name.death_year.isNull()) \
        .where(title.title_type.isin(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie'])) \
        .where(titleEpisode.title_id.isNull()) \
        .groupBy(name.id, name.primary_name, name.birth_year) \
        .agg(count('*').alias('roles')) \
        .orderBy(col('roles').desc()) \
        .select(name.id, name.primary_name, (year(current_date()) - name.birth_year).alias("age"), col("roles"))\
        .limit(100) \
        .collect()

def query3_dataframe_opt(name: DataFrame,
                    titlePrincipals: DataFrame,
                    titlePrincipalsCharacters: DataFrame,
                    category: DataFrame,
                    title: DataFrame,
                    titleEpisode: DataFrame
                    ) -> List[Row]:
    return name\
        .where(name.death_year.isNull()) \
        .join(titlePrincipals, name.id == titlePrincipals.name_id) \
        .join(titlePrincipalsCharacters, (titlePrincipalsCharacters.title_id == titlePrincipals.title_id) & (titlePrincipalsCharacters.name_id == titlePrincipals.name_id)) \
        .join(category, category.id == titlePrincipals.category_id) \
        .where(category.name == 'actress') \
        .join(title, title.id == titlePrincipals.title_id) \
        .where(title.start_year >= year(current_date()) - 10) \
        .where(title.title_type.isin(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie'])) \
        .groupBy(name.id, name.primary_name, name.birth_year) \
        .agg(count('*').alias('roles')) \
        .orderBy(col('roles').desc()) \
        .select(name.id, name.primary_name, (year(current_date()) - name.birth_year).alias("age"), col("roles"))\
        .limit(100) \
        .collect()

def main():
    @timeit
    def q1(title, userHistory, titleGenre, genre, titleAkas):
        result = query1_dataframe(title, userHistory, titleGenre, genre, titleAkas)
        df = spark.createDataFrame(result)
        df.show()

    @timeit
    def q2(title, titleEpisode, userhistory, users, titleGenre, genre):
        result = query2_dataframe(title, titleEpisode, userhistory, users, titleGenre, genre)
        df = spark.createDataFrame(result)
        df.show()
        
    @timeit
    def q3(name, titlePrincipals, titlePrincipalsCharacters, category, title, titleEpisode):
        # result = query3_dataframe(name, titlePrincipals, titlePrincipalsCharacters, category, title, titleEpisode)
        result = query3_dataframe_opt(name, titlePrincipals, titlePrincipalsCharacters, category, title, titleEpisode)
        df = spark.createDataFrame(result)
        df.show()

    if len(sys.argv) < 2:
        print('Missing function name. Usage: python3 main.py <function-name>')
        return
    elif sys.argv[1] not in locals():
        print(f'No such function: {sys.argv[1]}')
        return

    # the spark session
    spark = SparkSession.builder \
    .master("spark://spark:7077") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.executor.instances", 3) \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

    # data frames
    
    if sys.argv[1] == 'q1':
        title = spark.read.parquet(f"/app/parquets/title.parquet")
        # title.createOrReplaceTempView("title")
        
        userHistory = spark.read.parquet(f"/app/parquets/userhistory.parquet")
        # userhistory.createOrReplaceTempView("userHistory")
        
        titleGenre= spark.read.parquet(f"/app/parquets/titlegenre.parquet")
        # titleGenre.createOrReplaceTempView("titleGenre")

        genre = spark.read.parquet(f"/app/parquets/genre.parquet")
        # genre.createOrReplaceTempView("genre")
        
        titleAkas= spark.read.parquet(f"/app/parquets/titleakas.parquet")
        # titleAkas.createOrReplaceTempView("titleAkas")
        locals()[sys.argv[1]](title, userHistory, titleGenre, genre, titleAkas)
    
    if sys.argv[1] == 'q2':
        title = spark.read.parquet(f"/app/parquets/title.parquet")

        titleEpisode = spark.read.parquet(f"/app/parquets/titleepisode.parquet")

        userhistory = spark.read.parquet(f"/app/parquets/userhistory.parquet")

        users = spark.read.parquet(f"/app/parquets/users.parquet")

        titleGenre = spark.read.parquet(f"/app/parquets/titlegenre.parquet")

        genre = spark.read.parquet(f"/app/parquets/genre.parquet")

        locals()[sys.argv[1]](title, titleEpisode, userhistory, users, titleGenre, genre)
    
    if sys.argv[1].startswith('q3'):
        name = spark.read.parquet(f"/app/parquets/name.parquet")
        titleprincipals = spark.read.parquet(f"/app/parquets/titleprincipals.parquet")
        titleprincipalscharacters = spark.read.parquet(f"/app/parquets/titleprincipalscharacters.parquet")
        category = spark.read.parquet(f"/app/parquets/category.parquet")
        title = spark.read.parquet(f"/app/parquets/title.parquet")
        titleEpisode = spark.read.parquet(f"/app/parquets/titleepisode.parquet")
        
        locals()[sys.argv[1]](name, titleprincipals, titleprincipalscharacters, category, title, titleEpisode)
    




if __name__ == '__main__':
    main()
