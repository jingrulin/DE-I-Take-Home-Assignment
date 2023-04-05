#         i. The number of schools with college-going rates for Hispanic students

# Importing the libraries
import pandas as pd
import sqlite3

# Load xlsx file into DataFrame
df = pd.read_excel('output.xlsx')

# Create a connection to an in-memory SQLite database
with sqlite3.connect(':memory:') as conn:
    
    # Register DataFrame as a table in the SQLite database
    df.to_sql('mytable', conn, index=False)

    # Define the SQL query
    query = """
            SELECT COUNT(DISTINCT school_name) AS "num_schools"
            FROM mytable
            WHERE breakdown = 'Hispanic' AND value IS NOT NULL
            """

    # Execute query and return the result as a DataFrame
    result = pd.read_sql_query(query, conn)

# Print the result
print("There are {} schools with college-going rates for Hispanic students.".format(result['num_schools'][0]))

#        ii. The min and max school college-going rates, across all schools, for Asian students

# Importing the libraries
import pandas as pd
import sqlite3

# Load xlsx file into DataFrame
df = pd.read_excel('output.xlsx')

# Create a connection to an in-memory SQLite database
with sqlite3.connect(':memory:') as conn:
    
    # Register DataFrame as a table in the SQLite database
    df.to_sql('mytable', conn, index=False)

    # Define the SQL query
    query = """
            SELECT school_name, MIN(value) AS min_rate, MAX(value) AS max_rate
            FROM mytable
            WHERE breakdown = 'Asian' AND value IS NOT NULL
            GROUP BY school_name
            HAVING min_rate = MIN(value) OR max_rate = MAX(value)
            """

    # Execute query and return the result as a DataFrame
    result = pd.read_sql_query(query, conn)

# Print the results
min_rate = result['min_rate'].min()
max_rate = result['max_rate'].max()
min_school = result.loc[result['min_rate'] == min_rate, 'school_name'].iloc[0]
max_school = result.loc[result['max_rate'] == max_rate, 'school_name'].iloc[0]
print(f"The school with the minimum college-going rate for Asian students is {min_school} with a rate of {min_rate:.2f}%.") 
print(f"The school with the maximum college-going rate for Asian students is {max_school} with a rate of {max_rate:.2f}%.")

#        iii. The data coverage, by entity type, compared to last year (assume that the 2020 data also exists in your tables)

#Importing the Libraries
import pandas as pd
import sqlite3

# Load xlsx file into DataFrame
df = pd.read_excel('output.xlsx')

# Create a connection to an in-memory SQLite database
conn = sqlite3.connect(':memory:')

# Register DataFrame as a table in the SQLite database
df.to_sql('mytable', conn, index=False)

# Define the SQL query to calculate data coverage by entity type for the current year and last year
query = """
        SELECT entity_type, 
               SUM(CASE WHEN year = 2020 THEN 1 ELSE 0 END) AS last_year_count,
               SUM(CASE WHEN year = 2021 THEN 1 ELSE 0 END) AS current_year_count,
               AVG(CASE WHEN year = 2021 THEN value ELSE NULL END) AS current_year_avg,
               AVG(CASE WHEN year = 2020 THEN value ELSE NULL END) AS last_year_avg
        FROM mytable
        WHERE year IN (2020, 2021)
        GROUP BY entity_type
        """

# Execute query and return the result as a DataFrame
result = pd.read_sql_query(query, conn)

# Print the result
for index, row in result.iterrows():
    last_year_coverage = row['last_year_count'] / 365 # Assuming there are 365 days in a year
    current_year_coverage = row['current_year_count'] / 365 # Assuming there are 365 days in a year
    print("For entity type {}, data coverage for last year is {:.2%} with an average value of {:.2f}, and data coverage for current year is {:.2%} with an average value of {:.2f}.".format(row['entity_type'], last_year_coverage, row['last_year_avg'], current_year_coverage, row['current_year_avg']))
