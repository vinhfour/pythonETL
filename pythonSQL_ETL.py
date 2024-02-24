
# DB_HOST = 
# DB_NAME = 
# DB_USER = 
# DB_PASS = 

import os
import pandas as pd
import psycopg2 as ps
from sqlalchemy import create_engine

#conn = ps.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
#engine = create_engine()
connection = ps.connect(
host=os.environ['CLASS_DB_HOST'],
database=os.environ['CLASS_DB_USERNAME'],
user=os.environ['CLASS_DB_USERNAME'],
password=os.environ['CLASS_DB_PASSWORD'])
connection.autocommit = True

engine = create_engine(
f"postgresql://{os.environ['CLASS_DB_USERNAME']}:"
f"{os.environ['CLASS_DB_PASSWORD']}@"
f"{os.environ['CLASS_DB_HOST']}:"
f"5432/{os.environ['CLASS_DB_USERNAME']}"
)



main_df = pd.read_csv('Anime.csv').drop_duplicates(subset='Name')
warnings_df = main_df[['Content_Warning']]
tags_df = main_df[['Tags']]
mangas_df = main_df[['Related_Mange']]
tag_anime_df = main_df[['Name', 'Tags']]
warning_anime_df = main_df[['Name', 'Content_Warning']]
manga_anime_df = main_df[['Name', 'Related_Mange']]
anime_anime_df = main_df[['Name', 'Related_anime']]
studio_anime_df = main_df[['Name', 'Studio']]
anime_df = main_df[['Name','Japanese_name','Type', 'Episodes', 'Description', 'Release_season', 'Release_year', 'End_year', 'Rating']]
#print(main_df)

warnings = warnings_df.assign(Content_Warning=warnings_df.Content_Warning.str.split(",, ")).explode('Content_Warning').drop_duplicates().dropna()
tags = tags_df.assign(Tags=tags_df.Tags.str.replace(",, ", ", ").str.split(", ")).explode('Tags').drop_duplicates().dropna()
studios = main_df[['Studio']].drop_duplicates().dropna()
mangas = mangas_df.assign(Related_Mange=mangas_df.Related_Mange.str.split(", ")).explode('Related_Mange').drop_duplicates().dropna()
tag_rel = tag_anime_df.assign(Tags=tag_anime_df.Tags.str.replace(",, ", ", ").str.split(", ")).explode('Tags').dropna()
warning_rel = warning_anime_df.assign(Content_Warning=warning_anime_df.Content_Warning.str.split(",, ")).explode('Content_Warning').dropna()
manga_rel = manga_anime_df.assign(Related_Mange=manga_anime_df.Related_Mange.str.split(", ")).explode('Related_Mange').drop_duplicates().dropna()
anime_rel = anime_anime_df.assign(Related_anime=anime_anime_df.Related_anime.str.split(", ")).explode('Related_anime').drop_duplicates().dropna()
studio_rel = studio_anime_df.dropna()
anime = anime_df

anime = anime.rename(columns={"Name": "anime_name", "Japanese_name": "jp_name", "Type": "type", "Episodes": "episodes", "Description": "description", "Release_season": "release_season", "Release_year": "release_year",
"End_year":"end_year","Rating": "rating"})
print(anime)
print('# rows: ' + str(anime.shape[0]))
anime.to_sql('anime', engine, if_exists='append', index=False)


studios = studios.rename(columns={"Studio": "studio_name"})
print(studios)
print('# rows: ' + str(studios.shape[0]))
studios.to_sql('studios', engine, if_exists='append', index=False)

warnings = warnings.rename(columns={"Content_Warning": "warning"})
print(warnings)
print('# rows: ' + str(warnings.shape[0]))
warnings.to_sql('content_warnings', engine, if_exists='append', index=False)


tags = tags.rename(columns={"Tags": "tag_name"})
print(tags)
print('# rows: ' + str(tags.shape[0]))
tags.to_sql('tags', engine, if_exists='append', index=False)

mangas = mangas.rename(columns={"Related_Mange": "manga_name"})
print(mangas)
print('# rows: ' + str(mangas.shape[0]))
mangas.to_sql('mangas', engine, if_exists='append', index=False)


tag_rel = tag_rel.rename(columns={"Name": "anime_name", "Tags": "tag"})
print(tag_rel)
print('# rows: ' + str(tag_rel.shape[0]))
tag_rel.to_sql('tag_rel', engine, if_exists='append', index=False)


warning_rel = warning_rel.rename(columns={"Name": "anime_name", "Content_Warning": "content_warning"})
print(warning_rel)
print('# rows: ' + str(warning_rel.shape[0]))
warning_rel.to_sql('warning_rel', engine, if_exists='append', index=False)

manga_rel = manga_rel.rename(columns={"Name": "anime_name", "Related_Mange": "manga_name"})
print(manga_rel)
print('# rows: ' + str(manga_rel.shape[0]))
manga_rel.to_sql('manga_rel', engine, if_exists='append', index=False)

anime_rel = anime_rel.rename(columns={"Name": "anime_name", "Related_anime": "related_anime"})
print(anime_rel)
print('# rows: ' + str(anime_rel.shape[0]))
anime_rel.to_sql('anime_rel', engine, if_exists='append', index=False)

studio_rel = studio_rel.rename(columns={"Name": "anime_name", "Studio": "studio_name"})
print(studio_rel)
print('# rows: ' + str(studio_rel.shape[0]))
studio_rel.to_sql('studio_rel', engine, if_exists='append', index=False)

