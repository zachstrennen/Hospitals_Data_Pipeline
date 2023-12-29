import sys
import pandas as pd
import credentials
# install package psycopg2-binary
import psycopg2
import numpy as np

# Take in file name and date in file name from command line
date = sys.argv[1]
file_name = sys.argv[2]


# Read in given file from data file for Seymour_hospital table
hospitals = pd.read_csv(file_name)
# Create a new column called date
hospitals["date"] = date
# Replace all 'Not Available' strings to -1 so it can be converted
# -1's will be converted to NULL later
hospitals["Hospital overall rating"]\
    = hospitals["Hospital overall rating"].replace("Not Available", "-1")
# Convert ratings to Int64
hospitals["Hospital overall rating"]\
    = hospitals["Hospital overall rating"].astype('Int64')
# Change Emergency Services to a Boolean (TRUE or FALSE)
hospitals["Emergency Services"] =\
    pd.Series(np.where(hospitals[
                           "Emergency Services"
                       ].values == 'Yes', "TRUE", "FALSE"), hospitals[
        "Emergency Services"
    ].index)

# Create a separate data frame for Seymour_quality table
ratings = hospitals[["Facility ID", "date", "Hospital overall rating"]]

# Connect to SQL
conn = psycopg2.connect(
    host=credentials.get_hostname(), dbname=credentials.get_db(),
    user=credentials.get_username(), password=credentials.get_password()
)
cur = conn.cursor()

# Get the current Seymour_hospital table from the server and
# save it as a data frame
cur.execute("SELECT * FROM Seymour_hospital")
sql_hospital = pd.DataFrame(cur.fetchall(), columns=[
    "Facility ID",
    "Facility Name",
    "Address",
    "City",
    "State",
    "ZIP Code",
    "County Name",
    "Hospital Type",
    "Emergency Services"
])

hospital_gi = hospitals[[
    "Facility ID",
    "Facility Name",
    "Address",
    "City",
    "State",
    "ZIP Code",
    "County Name",
    "Hospital Type",
    "Emergency Services"
]]

# Get length of rows to find duplicates
gi_rows = len(hospital_gi.index)

# Create a new data frame to be inserted containing only new ID numbers
id_list = sql_hospital['Facility ID'].tolist()
hospital_gi = hospital_gi[~hospital_gi["Facility ID"].isin(id_list)]
# Remove lingering duplicates
hospital_gi = hospital_gi.drop_duplicates(subset=['Facility ID'])
# Fix indices for formatting
hospital_gi.reset_index(drop=True, inplace=True)

# Find the number of duplicates dropped
duplicates_dropped_gi = gi_rows - len(hospital_gi.index)

# Get the current Seymour_quality table from the server and save
# it as a data frame
cur.execute("SELECT * FROM Seymour_quality")
sql_quality = pd.DataFrame(cur.fetchall(), columns=[
    "Facility ID",
    "date",
    "Hospital overall rating"
])

# Get length of rows to find duplicates
ratings_rows = len(ratings.index)

# Create a new data frame to be inserted containing only new ID
# numbers and date pairings
exclusion_pairs = set(zip(sql_quality["Facility ID"].tolist(),
                          sql_quality["date"].apply(
                              lambda x: x.strftime('%Y-%m-%d')
                          ).tolist()))
ratings = ratings[~ratings[
    ['Facility ID', 'date']
].apply(tuple, axis=1).isin(exclusion_pairs)]
# Remove lingering duplicates
ratings = ratings.drop_duplicates(subset=['Facility ID', 'date'])
# Fix indices for formatting
ratings.reset_index(drop=True, inplace=True)

# Find the number of duplicates dropped
duplicates_dropped_ratings = ratings_rows - len(ratings.index)

# Placeholders for number of rows added and skipped
added_gi = 0
skipped_gi = 0

# Empty dataframe to store error rows (omissions)
omitted_hospitals_df = pd.DataFrame(columns=sql_hospital.columns)
omitted_hospitals_df = omitted_hospitals_df.astype(str)

# Map each variable into Seymour_hospital and insert the values
for i in range(len(hospital_gi)):
    try:
        cur.execute(
            """
                INSERT INTO Seymour_hospital
                (hospital_pk,
                hospital_name,
                address,
                city,
                state,
                zip_code,
                county,
                hospital_type,
                emergency_services)
                VALUES (%(hospital_pk)s,
                %(hospital_name)s,
                %(address)s,
                %(city)s,
                %(state)s,
                %(zip_code)s,
                %(county)s,
                %(hospital_type)s,
                %(emergency_services)s);
            """,
            {
                'hospital_pk': hospital_gi['Facility ID'][i],
                'hospital_name': hospital_gi['Facility Name'][i],
                'address': hospital_gi['Address'][i],
                'city': hospital_gi['City'][i],
                'state': hospital_gi['State'][i],
                'zip_code': hospital_gi['ZIP Code'][i].item(),
                'county': hospital_gi['County Name'][i],
                'hospital_type': hospital_gi['Hospital Type'][i],
                'emergency_services': hospital_gi['Emergency Services'][i]
            })
        # Add to count for successes
        added_gi = added_gi + 1
    except Exception:
        # Append the new failed row to a dataframe of omissions
        new_row = {'Facility ID': str(hospital_gi['Facility ID'][i]),
                   'Facility Name': str(hospital_gi['Facility Name'][i]),
                   'Address': str(hospital_gi['Address'][i]),
                   'City': str(hospital_gi['City'][i]),
                   'State': str(hospital_gi['State'][i]),
                   'ZIP Code': str(hospital_gi['ZIP Code'][i]),
                   'County Name': str(hospital_gi['County Name'][i]),
                   'Hospital Type': str(hospital_gi['Hospital Type'][i]),
                   'Emergency Services':
                       str(hospital_gi['Emergency Services'][i])
                   }
        omitted_hospitals_df.loc[len(omitted_hospitals_df)] = new_row
        # Export the omission dataframe to a csv
        omitted_hospitals_df.to_csv('omitted/omitted_hospitals.csv',
                                    index=False)
        # Add to count for failures
        skipped_gi = skipped_gi + 1

# Placeholders for number of rows added and skipped
added_ratings = 0
skipped_ratings = 0

# Empty dataframe to store error rows (omissions)
omitted_quality_df = pd.DataFrame(columns=sql_quality.columns)
omitted_quality_df = omitted_quality_df.astype(str)

# Map each variable into Seymour_quality and insert the values
for i in range(len(ratings)):
    try:
        cur.execute(
            """
                INSERT INTO Seymour_quality (hospital_pk, date, rating)
                VALUES (%(hospital_pk)s, %(date)s, %(rating)s);
            """,
            {
                'hospital_pk': ratings['Facility ID'][i],
                'date': str(ratings['date'][i]),
                'rating': ratings['Hospital overall rating'][i].item()
            })
        # Add to count for successes
        added_ratings = added_ratings + 1
    except Exception:
        # Append the new failed row to a dataframe of omissions
        new_row = {'hospital_pk': str(ratings['Facility ID'][i]),
                   'date': str(ratings['date'][i]),
                   'Hospital overall rating':
                       str(ratings['Hospital overall rating'][i])
                   }
        omitted_quality_df.loc[len(omitted_quality_df)] = new_row
        # Export the omission dataframe to a csv
        omitted_quality_df.to_csv('omitted/omitted_quality.csv', index=False)
        # Add to count for failures
        skipped_ratings = skipped_ratings + 1

# Set all rating values in Seymour quality that are -1 equal to NULL
cur.execute(
    """
        UPDATE Seymour_quality
        SET rating = NULL
        WHERE rating = -1
    """
)

# Close SQL
conn.commit()
conn.close()

# Print duplicates dropped
print(f"Dropped {duplicates_dropped_gi} duplicate rows of" +
      " hospital_pk before inserting into Seymour_hospital.")
print(f"Dropped {duplicates_dropped_ratings} duplicate rows of" +
      " hospital_pk/date pairings before inserting into Seymour_quality.")
print("")
# Print number of rows successfully inserted
print(f"Successfully inserted {added_gi}" +
      " rows to Seymour_hospital.")
print(f"Successfully inserted {added_ratings}" +
      " rows to Seymour_quality.")
print("")
# Print number of rows that raised errors
print(f"{skipped_gi} rows were not inserted" +
      " into Seymour_hospital due to errors.")
print(f"{skipped_ratings} rows were not inserted" +
      " into Seymour_quality due to errors.")
print("")
