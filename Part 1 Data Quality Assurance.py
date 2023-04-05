#Importing the Libraries
import pandas as pd

# Read data from the CSV files
enrollment_df = pd.read_csv("postsecondary_enrollment_current_2020-21.csv")
completion_df = pd.read_csv("hs_completion_certified_2020-21.csv")

# Print the first few rows of each DataFrame to inspect the data
print("Enrollment data:\n", enrollment_df.head())
print("\nCompletion data:\n", completion_df.head())

# Print the last few rows of each DataFrame to inspect the data
print("\nEnrollment data:\n", enrollment_df.tail())
print("\nCompletion data:\n", completion_df.tail())

# Check the data types of each column in the DataFrames
print("\nEnrollment data types:\n", enrollment_df.dtypes)
print("\nCompletion data types:\n", completion_df.dtypes)

# Check for missing values in the DataFrames
print("\nEnrollment missing values:\n", enrollment_df.isnull().sum())
print("\nCompletion missing values:\n", completion_df.isnull().sum())

# Check for duplicates in the DataFrames
print("\nEnrollment duplicates:\n", enrollment_df.duplicated().sum())
print("\nCompletion duplicates:\n", completion_df.duplicated().sum())

