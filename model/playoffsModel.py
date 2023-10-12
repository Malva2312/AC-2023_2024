import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier


def predictPlayoffs(db, table, target, split_year):
    database = "../" + db + ".db"
    conn = sqlite3.connect(database)
    query = "SELECT * FROM " + table + ";"
    data = pd.read_sql_query(query, conn)

    data["year"] = pd.to_numeric(data["year"])
    data = data.drop(
        [
            "lgID",
            "franchID",
            "tmID",
            "confID",
            "name",
            "firstRound",
            "arena",
            "semis",
            "finals",
        ],
        axis=1,
    )

    # manually spearating the data so it doesn't split mid-year
    train_data = data[data["year"] < split_year]
    test_data = data[data["year"] >= split_year]

    X_train = train_data.drop(["playoff"], axis=1)
    y_train = train_data["playoff"]

    X_test = test_data.drop(["playoff"], axis=1)
    y_test = test_data["playoff"]

    # escolher o modelo a utilizar
    model = DecisionTreeClassifier()
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    print(predictions)

    conn.close()


predictPlayoffs("database", "Teams", "playoff", 4)
