from datetime import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

spark = SparkSession.builder.appName("PlayerAnalysis").getOrCreate()

# Define file paths (adjust paths as needed for your local directory structure)
local_paths = {
    'player_valuations': '../data/raw/player_valuations.csv',
    'players': '../data/raw/players.csv',
    'game_events': '../data/raw/game_events.csv',
    'appearances': '../data/raw/appearances.csv'
}

# Load the CSV files into PySpark DataFrames
dfs = {}
for name, path in local_paths.items():
    dfs[name] = spark.read.option("header", "true").csv(path)

# Convert necessary columns to appropriate data types (for date columns)
dfs['players'] = dfs['players'].withColumn('date_of_birth', F.to_date(F.col('date_of_birth'), 'yyyy-MM-dd'))
dfs['appearances'] = dfs['appearances'].withColumn('date', F.to_date(F.col('date'), 'yyyy-MM-dd'))\
                                        .withColumn('goals', F.col('goals').cast('int'))\
                                        .withColumn('assists', F.col('assists').cast('int'))\
                                        .withColumn('minutes_played', F.col('minutes_played').cast('int'))\
                                        .withColumn('yellow_cards', F.col('yellow_cards').cast('int'))\
                                        .withColumn('red_cards', F.col('red_cards').cast('int'))

# Step 1: Create the 'age' column for players
players_with_age = dfs['players'].withColumn(
    'age',
    F.floor(F.datediff(F.current_date(), F.col('date_of_birth')) / 365.25)
)

# Step 2: Filter players between 16 and 23 who are also attackers
players_filtered = players_with_age.filter(
    (F.col('age') >= 16) & (F.col('age') <= 23) & (F.col('position') == 'Attack')
)

# Step 3: Separate players aged 16 to 18 and 19 to 23
players_16_to_18 = players_filtered.filter(F.col('age') <= 18)
players_19_to_23 = players_filtered.filter(F.col('age') >= 19)

# Step 4: Extract the year of each appearance for players 19+
players_appearances = players_filtered.join(dfs['appearances'], 'player_id').withColumn(
    'year',
    F.year('date')
)

# Step 5: Aggregate total appearances and distinct years to compute the average appearances per year
appearances_summary = players_appearances.groupBy('player_id').agg(
    F.sum('minutes_played').alias('total_minutes_played'),
    F.countDistinct('year').alias('distinct_years')
)

average_appearances = appearances_summary.withColumn(
    'average_minutes_per_year',
    F.col('total_minutes_played') / F.col('distinct_years')
)

# Step 6: Filter players who average 10 or more appearances per year
eligible_players = average_appearances.filter(
    F.col('average_minutes_per_year') >= 450
)


# Step 7: Combine players 16 to 18 with eligible players 19+
# eligible_players = players_16_to_18.select('player_id').union(enough_app_players.select('player_id'))
#
# eligible_players.filter(F.col('player_id') == '971570').show()

# Step 8: Group by player_id and get total goals, assists, and minutes played
total_ga = dfs['appearances'].groupBy('player_id').agg(
    F.sum('goals').alias('total_goals'),
    F.sum('assists').alias('total_assists'),
    F.sum('minutes_played').alias('total_minutes')
)

# Step 9: Normalize goals and assists per 90 minutes
norm_metrics = total_ga.withColumn(
    'goals_p90', (F.col('total_goals') / F.col('total_minutes')) * 90
).withColumn(
    'assists_p90', (F.col('total_assists') / F.col('total_minutes')) * 90
)

# Step 10: Join normalized metrics with eligible players and drop totals
norm_metrics = eligible_players.join(norm_metrics, 'player_id').drop('total_goals', 'total_assists')

# Step 11: Extract player metrics from the players DataFrame
players_metrics = dfs['players'].select('player_id', 'sub_position', 'foot', 'height_in_cm', 'contract_expiration_date')

# Step 12: Extract appearance metrics from the appearances DataFrame
appearances_metrics = dfs['appearances'].select('player_id', 'competition_id', 'yellow_cards', 'red_cards')

# Step 13: Combine normalized metrics with player and appearance data
combined_metrics = norm_metrics.join(players_metrics, 'player_id').join(appearances_metrics, 'player_id')

# Step 14: Compute yellows and reds per 90 minutes
combined_metrics = combined_metrics.withColumn(
    'yellows_p90', (F.col('yellow_cards') / F.col('total_minutes')) * 90
).withColumn(
    'reds_p90', (F.col('red_cards') / F.col('total_minutes')) * 90
)

# Step 15: Define European competitions for the bonus calculation
european_comps = ['EL', 'ELQ', 'ECLQ', 'CL', 'CLQ', 'USC', 'UCOL']

# Step 16: Filter for players who participated in European competitions
european_appearances = combined_metrics.filter(F.col('competition_id').isin(european_comps))

# Step 17: Count European appearances for each player
european_appearances_count = european_appearances.groupBy('player_id').count().withColumnRenamed('count', 'european_appearances')

# Step 18: Join European appearance counts to the combined metrics DataFrame
combined_metrics = combined_metrics.join(european_appearances_count, 'player_id', 'left_outer')

# Step 19: Create binary bonus columns
combined_metrics = combined_metrics.withColumn(
    'euro_bonus', F.when(F.col('european_appearances') >= 10, 1).otherwise(0)
).withColumn(
    'ambi_bonus', F.when(F.col('foot') == 'both', 1).otherwise(0)
).withColumn(
    'height_bonus', F.when((F.col('sub_position') != 'Right Winger') &
                           (F.col('sub_position') != 'Left Winger') &
                           (F.col('height_in_cm') > 182), 1).otherwise(0)
).withColumn(
    'contract_bonus', F.when(F.datediff(F.to_date(F.col('contract_expiration_date'), 'yyyy-MM-dd HH:mm:ss'), F.current_date()) < 365, 1).otherwise(0)
)

# Step 20: Aggregate metrics to ensure one row per player, computing averages
combined_metrics = combined_metrics.groupBy('player_id').agg(
    F.avg('goals_p90').alias('goals_p90'),
    F.avg('assists_p90').alias('assists_p90'),
    F.avg('yellows_p90').alias('yellows_p90'),
    F.avg('reds_p90').alias('reds_p90'),
    F.avg('euro_bonus').alias('euro_bonus'),
    F.avg('ambi_bonus').alias('ambi_bonus'),
    F.avg('height_bonus').alias('height_bonus'),
    F.avg('contract_bonus').alias('contract_bonus')
)

# Step 21: Join the final combined metrics with eligible players
final_df = eligible_players.join(combined_metrics, 'player_id')

# Step 22: Select the final columns
final_df = final_df.select(
    'player_id',
    'goals_p90',
    'assists_p90',
    'yellows_p90',
    'reds_p90',
    'euro_bonus',
    'ambi_bonus',
    'height_bonus',
    'contract_bonus'
)

# Define weights and bonuses
W_g = 5  # Weight for goals per 90
W_a = 3  # Weight for assists per 90
B_amb = 2  # Bonus for ambidextrous players
B_hei = 1  # Bonus for height
B_eu = 1  # Bonus for European competition appearances
B_con = 0.5  # Bonus for contract expiration
M_y = -1  # Penalty for yellow cards per 90
M_r = -5  # Penalty for red cards per 90

# Compute the gem score for each player
final_df = final_df.withColumn(
    'gem_score',
    (F.col('goals_p90') * W_g) +
    (F.col('assists_p90') * W_a) +
    (F.col('ambi_bonus') * B_amb) +
    (F.col('height_bonus') * B_hei) +
    (F.col('euro_bonus') * B_eu) +
    (F.col('contract_bonus') * B_con) +
    (F.col('yellows_p90') * M_y) +
    (F.col('reds_p90') * M_r)
)

# Find the top 5 players based on the gem score
top_5 = final_df.orderBy(F.col('gem_score').desc()).select('player_id').limit(5)

# Convert the top 5 player IDs into a list
top_5_list = [row.player_id for row in top_5.collect()]

# Extract details for the top 5 players from the players DataFrame
top_5_details = dfs['players'].filter(F.col('player_id').isin(top_5_list))

# Join the market value information into the final DataFrame
final_df = final_df.join(dfs['players'].select('player_id', 'market_value_in_eur'), 'player_id')

# Convert the final DataFrame to Pandas for plotting
final_df_pd = final_df.toPandas()

# Compute the log of the market value (to handle large value ranges)
final_df_pd['market_value_in_eur'] = pd.to_numeric(final_df_pd['market_value_in_eur'], errors='coerce')
final_df_pd['market_value_log'] = final_df_pd['market_value_in_eur'].apply(lambda x: np.log(x + 1))

plt.figure(figsize=(10, 6))
plt.scatter(final_df_pd['gem_score'], final_df_pd['market_value_log'])
plt.title('Performance Score vs Market Value')
plt.xlabel('Performance Score')
plt.ylabel('Log Market Value')
plt.grid(True)
plt.show()

# Join additional columns for building the recommender (e.g., name, sub_position, age, height_in_cm, image_url)
final_df = final_df.join(players_with_age.select('player_id', 'name', 'sub_position', 'age', 'height_in_cm', 'image_url', 'current_club_name', 'url'), 'player_id')

final_df_pd = final_df.toPandas()
final_df_pd['date'] = datetime.today().strftime('%Y-%m-%d')
final_df_pd.replace([np.inf, -np.inf], np.nan, inplace=True)
final_df_pd.fillna(0, inplace=True)

final_df_pd.to_csv('../data/final/final_df.csv', index=False)
