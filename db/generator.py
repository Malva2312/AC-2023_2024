import sqlite3
import csv
from helper import create_table_from_csv

database_name = "database.db"

conn = sqlite3.connect(database_name)
cursor = conn.cursor()
cursor.execute("PRAGMA foreign_keys = ON;")
conn.commit()
conn.close()

### Awards
awards = "Awards"
awards_csv_file = "../basketballPlayoffs/awards_players.csv"
awards_schema = '''
    CREATE TABLE IF NOT EXISTS Awards (
        playerID VARCHAR(255),
        award VARCHAR(255),
        year INTEGER,
        lgID VARCHAR(255)
    )
'''

### Coaches
coaches = "Coaches"
coaches_csv_file = "../basketballPlayoffs/coaches.csv"
coaches_schema = '''
    CREATE TABLE IF NOT EXISTS Coaches (
        coachID VARCHAR(255),
        year INTEGER,
        tmID VARCHAR(255),
        lgID VARCHAR(255),
        stint INTEGER,
        won INTEGER,
        lost INTEGER,
        post_wins INTEGER,
        post_losses INTEGER,
        PRIMARY KEY(coachID, stint)
    )
'''

### Player Teams
player_teams = "Player_Teams"
players_teams_csv_file = "../basketballPlayoffs/players_teams.csv"
players_teams_schema = '''
    CREATE TABLE IF NOT EXISTS Player_Teams (
        playerID VARCHAR(255),
        year INTEGER,
        stint INTEGER,
        tmID VARCHAR(255),
        lgID VARCHAR(255),
        GP INTEGER,
        GS INTEGER,
        minutes INTEGER,
        points INTEGER,
        oRebounds INTEGER,
        dRebounds INTEGER,
        rebounds INTEGER,
        assists INTEGER,
        steals INTEGER,
        blocks INTEGER,
        turnovers INTEGER,
        PF INTEGER,
        fgAttempted INTEGER,
        fgMade INTEGER,
        ftAttempted INTEGER,
        ftMade INTEGER,
        threeAttempted INTEGER,
        threeMade INTEGER,
        dq INTEGER,
        PostGP INTEGER,
        PostGS INTEGER,
        PostMinutes INTEGER,
        PostPoints INTEGER,
        PostoRebounds INTEGER,
        PostdRebounds INTEGER,
        PostRebounds INTEGER,
        PostAssists INTEGER,
        PostSteals INTEGER,
        PostBlocks INTEGER,
        PostTurnovers INTEGER,
        PostPF INTEGER,
        PostfgAttempted INTEGER,
        PostfgMade INTEGER,
        PostftAttempted INTEGER,
        PostftMade INTEGER,
        PostthreeAttempted INTEGER,
        PostthreeMade INTEGER,
        PostDQ INTEGER
    )
'''

### Players
players = "Players"
players_csv_file = "../basketballPlayoffs/players.csv"
players_schema = '''
    CREATE TABLE IF NOT EXISTS Players (
        bioID VARCHAR(255),
        pos VARCHAR(255),
        firstseason INTEGER,
        lastseason INTEGER,
        height VARCHAR(255),
        weight INTEGER,
        college VARCHAR(255),
        collegeOther VARCHAR(255),
        birthDate DATE,
        deathDate DATE
    )
'''

### Series Post
series_post = "Series_Post"
series_post_csv_file = "../basketballPlayoffs/series_post.csv"
series_post_schema = '''
    CREATE TABLE IF NOT EXISTS Series_Post (
        year INTEGER,
        round INTEGER,
        series VARCHAR(255),
        tmIDWinner VARCHAR(255),
        lgIDWinner VARCHAR(255),
        tmIDLoser VARCHAR(255),
        lgIDLoser VARCHAR(255),
        W INTEGER,
        L INTEGER
    )
'''

###  Teams Post
teams_post = "Teams_Post"
teams_post_csv_file = "../basketballPlayoffs/teams_post.csv"
teams_post_schema = '''
    CREATE TABLE IF NOT EXISTS Teams_Post (
        year INTEGER,
        tmID VARCHAR(255),
        lgID VARCHAR(255),
        W INTEGER,
        L INTEGER
    )
'''

### Teams
teams = "Teams"
teams_csv_file = "../basketballPlayoffs/teams.csv"
teams_schema = '''
    CREATE TABLE IF NOT EXISTS Teams (
        year INTEGER,
        lgID VARCHAR(255),
        tmID VARCHAR(255),
        franchID VARCHAR(255),
        confID VARCHAR(255),
        divID VARCHAR(255),
        rank INTEGER,
        playoff INTEGER,
        seeded INTEGER,
        firstRound INTEGER,
        semis INTEGER,
        finals INTEGER,
        name VARCHAR(255),
        o_fgm INTEGER,
        o_fga INTEGER,
        o_ftm INTEGER,
        o_fta INTEGER,
        o_3pm INTEGER,
        o_3pa INTEGER,
        o_oreb INTEGER,
        o_dreb INTEGER,
        o_reb INTEGER,
        o_asts INTEGER,
        o_pf INTEGER,
        o_stl INTEGER,
        o_to INTEGER,
        o_blk INTEGER,
        o_pts INTEGER,
        d_fgm INTEGER,
        d_fga INTEGER,
        d_ftm INTEGER,
        d_fta INTEGER,
        d_3pm INTEGER,
        d_3pa INTEGER,
        d_oreb INTEGER,
        d_dreb INTEGER,
        d_reb INTEGER,
        d_asts INTEGER,
        d_pf INTEGER,
        d_stl INTEGER,
        d_to INTEGER,
        d_blk INTEGER,
        d_pts INTEGER,
        tmORB INTEGER,
        tmDRB INTEGER,
        tmTRB INTEGER,
        opptmORB INTEGER,
        opptmDRB INTEGER,
        opptmTRB INTEGER,
        won INTEGER,
        lost INTEGER,
        GP INTEGER,
        homeW INTEGER,
        homeL INTEGER,
        awayW INTEGER,
        awayL INTEGER,
        confW INTEGER,
        confL INTEGER,
        min INTEGER,
        attend INTEGER,
        arena VARCHAR(255),
        PRIMARY KEY (year, tmID)
    )
'''

names = [
    awards,
    coaches,
    player_teams,
    players,
    series_post,
    teams_post,
    teams
    ]

csv_file = [
    awards_csv_file,
    coaches_csv_file,
    players_teams_csv_file,
    players_csv_file,
    series_post_csv_file,
    teams_post_csv_file,
    teams_csv_file
    ]

schemas = [
    awards_schema,
    coaches_schema,
    players_teams_schema,
    players_schema,
    series_post_schema,
    teams_post_schema,
    teams_schema
    ]


for idx in range(0, len(names)):
    create_table_from_csv(database_name, names[idx], [schemas[idx]], csv_file[idx])