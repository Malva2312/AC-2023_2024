import sqlite3
import csv
import pandas as pd


def create_db(database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # List of create table queries
    create_table_queries = [
        '''
        DROP TABLE IF EXISTS Awards_Players;
        CREATE TABLE Awards_Players (
            playerID TEXT,
            award TEXT,
            year INTEGER,
            lgID TEXT
        );
        ''',
        '''
        DROP TABLE IF EXISTS Coaches;
        CREATE TABLE Coaches (
            coachID TEXT,
            year INTEGER,
            tmID TEXT,
            lgID TEXT,
            stint INTEGER,
            won INTEGER,
            lost INTEGER,
            post_wins INTEGER,
            post_losses INTEGER
        );
        ''',
        '''
        DROP TABLE IF EXISTS Players_Teams;
        CREATE TABLE Players_Teams (
            playerID TEXT,
            year INTEGER,
            stint INTEGER,
            tmID TEXT,
            lgID TEXT,
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
        );
        ''',
        '''
        DROP TABLE IF EXISTS Players;
        CREATE TABLE Players (
            bioID TEXT,
            pos TEXT,
            firstseason INTEGER,
            lastseason INTEGER,
            height REAL,
            weight INTEGER,
            college TEXT,
            collegeOther TEXT,
            birthDate TEXT,
            deathDate TEXT
        );
        ''',
        '''
        DROP TABLE IF EXISTS Series_Post;
        CREATE TABLE Series_Post (
            year INTEGER,
            round TEXT,
            series TEXT,
            tmIDWinner TEXT,
            lgIDWinner TEXT,
            tmIDLoser TEXT,
            lgIDLoser TEXT,
            W INTEGER,
            L INTEGER
        );
        ''',
        '''
        DROP TABLE IF EXISTS Teams_Post;
        CREATE TABLE Teams_Post (
            year INTEGER,
            tmID TEXT,
            lgID TEXT,
            W INTEGER,
            L INTEGER
        );
        ''',
        '''
        DROP TABLE IF EXISTS Teams;
        CREATE TABLE Teams (
            year INTEGER,
            lgID TEXT,
            tmID TEXT,
            franchID TEXT,
            confID TEXT,
            divID TEXT,
            rank INTEGER,
            playoff TEXT,
            seeded INTEGER,
            firstRound TEXT,
            semis TEXT,
            finals TEXT,
            name TEXT,
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
            arena TEXT
        );
        '''
    ]

    # Execute each query
    for query in create_table_queries:
        cursor.executescript(query)

    # Commit changes and close the connection
    conn.commit()
    conn.close()

    # Define a dictionary with table names and their corresponding attributes
    tables_and_attributes = {
        "Awards_Players": ["playerID", "award", "year", "lgID"],
        "Coaches": ["coachID", "year", "tmID", "lgID", "stint", "won", "lost", "post_wins", "post_losses"],
        "Players_Teams": ["playerID", "year", "stint", "tmID", "lgID", "GP", "GS", "minutes", "points", "oRebounds", "dRebounds", "rebounds", "assists", "steals", "blocks", "turnovers", "PF", "fgAttempted", "fgMade", "ftAttempted", "ftMade", "threeAttempted", "threeMade", "dq", "PostGP", "PostGS", "PostMinutes", "PostPoints", "PostoRebounds", "PostdRebounds", "PostRebounds", "PostAssists", "PostSteals", "PostBlocks", "PostTurnovers", "PostPF", "PostfgAttempted", "PostfgMade", "PostftAttempted", "PostftMade", "PostthreeAttempted", "PostthreeMade", "PostDQ"],
        "Players": ["bioID", "pos", "firstseason", "lastseason", "height", "weight", "college", "collegeOther", "birthDate", "deathDate"],
        "Series_Post": ["year", "round", "series", "tmIDWinner", "lgIDWinner", "tmIDLoser", "lgIDLoser", "W", "L"],
        "Teams_Post": ["year", "tmID", "lgID", "W", "L"],
        "Teams": ["year", "lgID", "tmID", "franchID", "confID", "divID", "rank", "playoff", "seeded", "firstRound", "semis", "finals", "name", "o_fgm", "o_fga", "o_ftm", "o_fta", "o_3pm", "o_3pa", "o_oreb", "o_dreb", "o_reb", "o_asts", "o_pf", "o_stl", "o_to", "o_blk", "o_pts", "d_fgm", "d_fga", "d_ftm", "d_fta", "d_3pm", "d_3pa", "d_oreb", "d_dreb", "d_reb", "d_asts", "d_pf", "d_stl", "d_to", "d_blk", "d_pts", "tmORB", "tmDRB", "tmTRB", "opptmORB", "opptmDRB", "opptmTRB", "won", "lost", "GP", "homeW", "homeL", "awayW", "awayL", "confW", "confL", "min", "attend", "arena"]
    }

    return tables_and_attributes


def load_data_db(csv_file, table_name, database_name):
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()

    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip the header row
        for row in csv_reader:
            query = f"INSERT INTO {table_name} VALUES ({','.join(['?'] * len(row))})"
            cur.execute(query, row)

    conn.commit()
    conn.close()


def original_schema():
    # Define the schemas for each table
    awards_players_schema = """
    CREATE TABLE Awards_Players (
        playerID TEXT,
        award TEXT,
        year INTEGER,
        lgID TEXT
    );
    """

    coaches_schema = """
    CREATE TABLE Coaches (
        coachID TEXT,
        year INTEGER,
        tmID TEXT,
        lgID TEXT,
        stint INTEGER,
        won INTEGER,
        lost INTEGER,
        post_wins INTEGER,
        post_losses INTEGER
    );
    """

    players_teams_schema = """
    CREATE TABLE Players_Teams (
        playerID TEXT,
        year INTEGER,
        stint INTEGER,
        tmID TEXT,
        lgID TEXT,
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
    );
    """

    players_schema = """
    CREATE TABLE Players (
        bioID TEXT,
        pos TEXT,
        firstseason INTEGER,
        lastseason INTEGER,
        height REAL,
        weight INTEGER,
        college TEXT,
        collegeOther TEXT,
        birthDate TEXT,
        deathDate TEXT
    );
    """

    series_post_schema = """
    CREATE TABLE Series_Post (
        year INTEGER,
        round TEXT,
        series TEXT,
        tmIDWinner TEXT,
        lgIDWinner TEXT,
        tmIDLoser TEXT,
        lgIDLoser TEXT,
        W INTEGER,
        L INTEGER
    );
    """

    teams_post_schema = """
    CREATE TABLE Teams_Post (
        year INTEGER,
        tmID TEXT,
        lgID TEXT,
        W INTEGER,
        L INTEGER
    );
    """

    teams_schema = """
    CREATE TABLE Teams (
        year INTEGER,
        lgID TEXT,
        tmID TEXT,
        franchID TEXT,
        confID TEXT,
        divID TEXT,
        rank INTEGER,
        playoff TEXT,
        seeded INTEGER,
        firstRound TEXT,
        semis TEXT,
        finals TEXT,
        name TEXT,
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
        arena TEXT
    );
    """

    # Create a dictionary with table names as keys and their corresponding schemas as values
    schema_dictionary = {
        'Awards_Players': awards_players_schema,
        'Coaches': coaches_schema,
        'Players_Teams': players_teams_schema,
        'Players': players_schema,
        'Series_Post': series_post_schema,
        'Teams_Post': teams_post_schema,
        'Teams': teams_schema
    }

    return schema_dictionary


def alter_schema_drop_columns(schema, columns_to_drop):
    schema = f", {schema}, "  # Add delimiters at the start and end
    for col in columns_to_drop:
        schema = schema.replace(f", {col} INTEGER, ", ",").replace(f", {col} INTEGER ", ",")
        schema = schema.replace(f", {col} REAL, ", ",").replace(f", {col} REAL ", ",")
        schema = schema.replace(f", {col} TEXT, ", ",").replace(f", {col} TEXT ", ",")
        schema = schema.replace(f", {col} BLOB, ", ",").replace(f", {col} BLOB ", ",")
        schema = schema.replace(f", ,", ",")  # Remove any double commas
        schema = schema.strip(", ")  # Remove any leading or trailing commas and spaces
    return schema




def retrieve_data(database_name, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_name)
    
    # Query to fetch data from the specified table
    query = f"SELECT * FROM {table_name}"
    
    # Load data from the database into a Pandas DataFrame
    data_df = pd.read_sql_query(query, conn)
    
    # Close the connection
    conn.close()
    
    return data_df


def insert_dataframe(dataframe, table_name, database_name, schema):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_name)
    
    # Drop the table if it already exists
    conn.execute(f'DROP TABLE IF EXISTS {table_name}')
    
    # Create a new table with the cleaned up schema
    conn.execute(schema)
    
    # Use the dataframe to populate the new table
    dataframe.to_sql(table_name, conn, index=False, if_exists='append')
    
    # Close the connection
    conn.close()

def schema_builder(dataframe):
    # Get the column names from the DataFrame
    columns = dataframe.columns
    
    # Construct the schema string
    schema = ""
    for col in columns:
        # Modify column names with spaces or special characters
        modified_col = col.replace(' ', '_')
        modified_col = modified_col.replace('-', '_')
        modified_col = modified_col.replace('(', '')
        modified_col = modified_col.replace(')', '')

        if dataframe[col].dtype == 'O':
            schema += f"{modified_col} TEXT, "
        elif dataframe[col].dtype == 'int64':
            schema += f"{modified_col} INTEGER, "
        elif dataframe[col].dtype == 'float64':
            schema += f"{modified_col} REAL, "
        else:
            schema += f"{modified_col} BLOB, "

    # Remove the trailing comma and space from the last column entry
    schema = schema[:-2]

    return schema


def copy_missing_tables(source_db, dest_db):
    # Connect to the source database
    source_conn = sqlite3.connect(source_db)
    source_cursor = source_conn.cursor()

    # Connect to the destination database
    dest_conn = sqlite3.connect(dest_db)
    dest_cursor = dest_conn.cursor()

    # Fetch the table names from the source database
    source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = source_cursor.fetchall()

    # Copy the missing tables
    for table in tables:
        table_name = table[0]
        dest_cursor.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if dest_cursor.fetchone()[0] == 0:
            source_cursor.execute(f"SELECT * FROM {table_name}")
            rows = source_cursor.fetchall()
            schema_query = f"PRAGMA table_info({table_name})"
            source_cursor.execute(schema_query)
            schema = source_cursor.fetchall()
            schema_string = ','.join([f"{col[1]} {col[2]}" for col in schema])
            dest_cursor.execute(f"CREATE TABLE {table_name} ({schema_string})")
            dest_cursor.executemany(f"INSERT INTO {table_name} VALUES ({','.join(['?'] * len(rows[0]))})", rows)

    # Commit the changes and close the connections
    dest_conn.commit()
    source_conn.close()
    dest_conn.close()
