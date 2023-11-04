import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report



def execute_sql_query_and_fetch_df(database_path, attributes, table_names, join_conditions):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)

    # Generate the select and join clauses dynamically
    select_clause = ", ".join(attributes)
    join_clause = f"{table_names[0]} t1"
    for i in range(1, len(table_names)):
        join_clause += f" JOIN {table_names[i]} t{i+1} ON {join_conditions[i-1]}"

    # Create the complete query string
    query = f"SELECT {select_clause} FROM {join_clause}"

    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()

    # Convert the fetched data to a pandas dataframe
    df = pd.DataFrame(rows, columns=[attribute.split('.')[1] for attribute in attributes])

    # Close the connection
    conn.close()

    return df


# Plot the evaluation metrics over the years
def plot_metrics_over_years(all_results, start, total_years):
    for m in all_results:
        plt.figure()
        years = list(range(start, total_years + 1))
        plt.plot(years, all_results[m], marker='o')
        plt.title(f'{m} over the years')
        plt.xlabel('Year')
        plt.ylabel(m)
        plt.show()


#Sliding window
def sliding_window_analysis(df, start, total_years, window_size, target, metrics, pipeline):
    results = dict()
    all_results = {m: [] for m in metrics.keys()}

    for i in range(total_years - window_size):
        # Split the data into train and test sets
        train = df[(df['year'] >= start + i) & (df['year'] < start + i + window_size)]
        test = df[df['year'] == start + i + window_size]

        x_train = train.drop(target, axis=1)
        y_train = train[target]
        x_test = test.drop(target, axis=1)
        y_test = test[target]

        # Fit the pipeline to the training data
        pipeline.fit(x_train, y_train)

        # Predict the test data
        y_pred = pipeline.predict(x_test)
        #print(classification_report(y_test, y_pred))

        # Store the results
        for m in metrics.keys():
            if m not in results:
                results[m] = []
            results[m] = metrics[m](y_test, y_pred)

        for m in metrics.keys():
            all_results[m].append(metrics[m](y_test, y_pred))
    return all_results


# Expanding window
def expanding_window_analysis(df, start, total_years, target, metrics, pipeline):
    results = dict()
    all_results = {m: [] for m in metrics.keys()}

    for i in range(start, total_years + 1):
        # Split the data into train and test sets
        train = df[df['year'] < i]
        test = df[df['year'] == i]

        x_train = train.drop(target, axis=1)
        y_train = train[target]
        x_test = test.drop(target, axis=1)
        y_test = test[target]

        # Fit the pipeline to the training data
        pipeline.fit(x_train, y_train)

        # Predict the test data
        y_pred = pipeline.predict(x_test)
        #print(classification_report(y_test, y_pred))

        # Store the results
        for m in metrics.keys():
            if m not in results:
                results[m] = []
            results[m].append(metrics[m](y_test, y_pred))

        for m in metrics.keys():
            all_results[m].append(metrics[m](y_test, y_pred))
    return all_results


# Mixed window
def mixed_window_analysis(df, total_years, start, start_sliding, metrics, pipeline, target):
    results = dict()
    all_results = {m: [] for m in metrics.keys()}

    for i in range(2, total_years + 1):
        # Expanding window
        if i < start_sliding:
            window_size = i + 1
            train = df[df['year'] < i]

        # Sliding window
        else:
            window_size = start_sliding
            train = df[(df['year'] >= i - window_size) & (df['year'] < i)]

        test = df[df['year'] == i]

        x_train = train.drop(target, axis=1)
        y_train = train[target]
        x_test = test.drop(target, axis=1)
        y_test = test[target]

        # Fit the pipeline to the training data
        pipeline.fit(x_train, y_train)
        #print(classification_report(y_test, y_pred))
        
        # Predict the test data
        y_pred = pipeline.predict(x_test)

        # Store the results
        for m in metrics.keys():
            if m not in results:
                results[m] = []
            results[m].append(metrics[m](y_test, y_pred))

        for m in metrics.keys():
            all_results[m].append(metrics[m](y_test, y_pred))

    return results, all_results