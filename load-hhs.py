import sys
import pandas as pd
import credentials
# install package psycopg2-binary
import psycopg2

# Get name of file from commandline
file_name = sys.argv[1]

# Read in given file from data file for Seymour_weekly table
hospital_weekly = pd.read_csv(file_name)

# Select specific columns from hospital_weekly that will be added
# to Seymour_geo
# Store these columns in a data frame
hospital_geo = hospital_weekly[[
    "hospital_pk",
    "fips_code",
    "geocoded_hospital_address"
]]

# Remove all duplicate hospital_pk values from hospital_geo
hospital_geo = hospital_geo.drop_duplicates(subset=["hospital_pk"])

# Connect to SQL
conn = psycopg2.connect(
    host=credentials.get_hostname(), dbname=credentials.get_db(),
    user=credentials.get_username(), password=credentials.get_password()
)
cur = conn.cursor()

# Get the current Seymour_weekly_info from the server and save it
# as a data frame
cur.execute("SELECT * FROM Seymour_weekly_info")
sql_weekly = pd.DataFrame(cur.fetchall(), columns=[
    "hospital_pk",
    "collection_week",
    "all_adult_hospital_beds_7_day_avg",
    "all_pediatric_inpatient_beds_7_day_avg",
    "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
    "all_pediatric_inpatient_bed_occupied_7_day_avg",
    "total_icu_beds_7_day_avg",
    "icu_beds_used_7_day_avg",
    "inpatient_beds_used_covid_7_day_avg",
    "staffed_icu_adult_patients_confirmed_covid_7_day_avg"
])

# Get length of rows to find duplicates
weekly_rows = len(hospital_weekly.index)

# Check that there are no existing pairs of hospital_pk and collection_week
exclusion_pairs = set(zip(sql_weekly["hospital_pk"].tolist(),
                          sql_weekly["collection_week"]
                          .apply(lambda x: x.strftime('%Y-%m-%d'))
                          .tolist()))
# Remove existing pairs as needed
hospital_weekly = hospital_weekly[
    ~hospital_weekly[
        ["hospital_pk", "collection_week"]
    ].apply(tuple, axis=1).isin(exclusion_pairs)
]
# Fix indices for formatting
hospital_weekly.reset_index(drop=True, inplace=True)

# Find the number of duplicates dropped
duplicates_dropped_weekly = weekly_rows - len(hospital_weekly.index)

# Get the current Seymour_geo from the server and save it as a data frame
cur.execute("SELECT * FROM Seymour_geo")
sql_geo = pd.DataFrame(cur.fetchall(), columns=[
    "hospital_pk",
    "fips_code",
    "hosptial_geocode"
])

# Get length of rows to find duplicates
geo_rows = len(hospital_geo.index)

# Only add non-existing hospital_pk values to the table
id_list = sql_geo["hospital_pk"].tolist()
# Remove existing hospital_pk values
hospital_geo = hospital_geo[~hospital_geo["hospital_pk"].isin(id_list)]
# Fix indices for formatting
hospital_geo.reset_index(drop=True, inplace=True)

# Find the number of duplicates dropped
duplicates_dropped_geo = geo_rows - len(hospital_geo.index)

# Placeholders for number of rows added and skipped
added_weekly = 0
skipped_weekly = 0

# Empty dataframe to store error rows (omissions)
omitted_weekly_df = pd.DataFrame(columns=sql_weekly.columns)
omitted_weekly_df = omitted_weekly_df.astype(str)

# Map each variable into Seymour_weekly_info and insert the values
for i in range(len(hospital_weekly)):
    try:
        cur.execute(
            """
                INSERT INTO Seymour_weekly_info
                (hospital_pk,
                collection_week,
                all_adult_hospital_beds_7_day_avg,
                all_pediatric_inpatient_beds_7_day_avg,
                all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
                all_pediatric_inpatient_bed_occupied_7_day_avg,
                total_icu_beds_7_day_avg,
                icu_beds_used_7_day_avg,
                inpatient_beds_used_covid_7_day_avg,
                staffed_icu_adult_patients_confirmed_covid_7_day_avg)
                VALUES
                (%(hospital_pk)s,
                %(collection_week)s,
                %(all_adult_hospital_beds_7_day_avg)s,
                %(all_pediatric_inpatient_beds_7_day_avg)s,
                %(all_adult_hospital_inpatient_bed_occupied_7_day_coverage)s,
                %(all_pediatric_inpatient_bed_occupied_7_day_avg)s,
                %(total_icu_beds_7_day_avg)s,
                %(icu_beds_used_7_day_avg)s,
                %(inpatient_beds_used_covid_7_day_avg)s,
                %(staffed_icu_adult_patients_confirmed_covid_7_day_avg)s);
            """,
            {
                'hospital_pk':
                hospital_weekly['hospital_pk'][i],
                'collection_week':
                hospital_weekly['collection_week'][i],
                'all_adult_hospital_beds_7_day_avg':
                hospital_weekly['all_adult_hospital_beds_7_day_avg'][i].item(),
                'all_pediatric_inpatient_beds_7_day_avg':
                hospital_weekly[
                    'all_pediatric_inpatient_beds_7_day_avg'
                ][i].item(),
                'all_adult_hospital_inpatient_bed_occupied_7_day_coverage':
                hospital_weekly[
                    'all_adult_hospital_inpatient_bed_occupied_7_day_coverage'
                ][i].item(),
                'all_pediatric_inpatient_bed_occupied_7_day_avg':
                hospital_weekly[
                    'all_pediatric_inpatient_bed_occupied_7_day_avg'
                ][i].item(),
                'total_icu_beds_7_day_avg':
                hospital_weekly['total_icu_beds_7_day_avg'][i].item(),
                'icu_beds_used_7_day_avg':
                hospital_weekly['icu_beds_used_7_day_avg'][i].item(),
                'inpatient_beds_used_covid_7_day_avg':
                hospital_weekly[
                    'inpatient_beds_used_covid_7_day_avg'
                ][i].item(),
                'staffed_icu_adult_patients_confirmed_covid_7_day_avg':
                hospital_weekly[
                    'staffed_icu_adult_patients_confirmed_covid_7_day_avg'
                ][i].item()
            })
        # Add to count for successes
        added_weekly = added_weekly + 1
    except Exception:
        # Append the new failed row to a dataframe of omissions
        new_row = {'hospital_pk': str(hospital_weekly['hospital_pk'][i]),
                   'collection_week':
                       str(hospital_weekly['collection_week'][i]),
                   'all_adult_hospital_beds_7_day_avg':
                       str(hospital_weekly[
                               'all_adult_hospital_beds_7_day_avg'
                           ][i]),
                   'all_pediatric_inpatient_beds_7_day_avg':
                       str(hospital_weekly[
                               'all_pediatric_inpatient_beds_7_day_avg'
                           ][i]),
                   'all_adult_hospital_inpatient_bed_occupied_7_day_coverage':
                       str(hospital_weekly[
                        'all_adult_hospital_inpatient_\
                            bed_occupied_7_day_coverage'
                           ][i]),
                   'all_pediatric_inpatient_bed_occupied_7_day_avg':
                       str(hospital_weekly[
                            'all_pediatric_inpatient_bed_occupied_7_day_avg'
                           ][i]),
                   'total_icu_beds_7_day_avg':
                       str(hospital_weekly['total_icu_beds_7_day_avg'][i]),
                   'icu_beds_used_7_day_avg':
                       str(hospital_weekly['icu_beds_used_7_day_avg'][i]),
                   'inpatient_beds_used_covid_7_day_avg':
                       str(hospital_weekly[
                               'inpatient_beds_used_covid_7_day_avg'
                           ][i]),
                   'staffed_icu_adult_patients_confirmed_covid_7_day_avg':
                       str(hospital_weekly[
                            'staffed_icu_adult_patients_\
                                confirmed_covid_7_day_avg'
                           ][i])
                   }
        omitted_weekly_df.loc[len(omitted_weekly_df)] = new_row
        # Export the omission dataframe to a csv
        omitted_weekly_df.to_csv('omitted/omitted_weekly.csv', index=False)
        # Add to count for failures
        skipped_weekly = skipped_weekly + 1

# Change all negative values to NULL for the 8 following variables
cur.execute(
    """
        UPDATE Seymour_weekly_info
        SET all_adult_hospital_beds_7_day_avg = NULL
        WHERE all_adult_hospital_beds_7_day_avg < 0
    """
)
cur.execute(
    """
        UPDATE Seymour_weekly_info
        SET all_pediatric_inpatient_beds_7_day_avg = NULL
        WHERE all_pediatric_inpatient_beds_7_day_avg < 0
    """
)
cur.execute(
    """
        UPDATE Seymour_weekly_info
        SET all_adult_hospital_inpatient_bed_occupied_7_day_coverage = NULL
        WHERE all_adult_hospital_inpatient_bed_occupied_7_day_coverage < 0
    """
)
cur.execute(
    """
        UPDATE Seymour_weekly_info
        SET all_pediatric_inpatient_bed_occupied_7_day_avg = NULL
        WHERE all_pediatric_inpatient_bed_occupied_7_day_avg < 0
    """
)
cur.execute(
    """
        UPDATE Seymour_weekly_info
        SET total_icu_beds_7_day_avg = NULL
        WHERE total_icu_beds_7_day_avg < 0
    """
)
cur.execute(
    """
        UPDATE Seymour_weekly_info
        SET icu_beds_used_7_day_avg = NULL
        WHERE icu_beds_used_7_day_avg < 0
    """
)
cur.execute(
    """
        UPDATE Seymour_weekly_info
        SET inpatient_beds_used_covid_7_day_avg = NULL
        WHERE inpatient_beds_used_covid_7_day_avg < 0
    """
)
cur.execute(
    """
        UPDATE Seymour_weekly_info
        SET staffed_icu_adult_patients_confirmed_covid_7_day_avg = NULL
        WHERE staffed_icu_adult_patients_confirmed_covid_7_day_avg < 0
    """
)

# Placeholders for number of rows added and skipped
added_geo = 0
skipped_geo = 0

# Empty dataframe to store error rows (omissions)
omitted_geo_df = pd.DataFrame(columns=sql_geo.columns)
omitted_geo_df = omitted_geo_df.astype(str)

# Map each variable into Seymour_geo and insert the values
for i in range(len(hospital_geo)):
    try:
        cur.execute(
            """
                INSERT INTO Seymour_geo
                (hospital_pk,
                fips_code,
                hosptial_geocode)
                VALUES (%(hospital_pk)s,
                %(fips_code)s,
                %(hosptial_geocode)s);
            """,
            {
                'hospital_pk': hospital_geo['hospital_pk'][i],
                'fips_code': hospital_geo['fips_code'][i].item(),
                'hosptial_geocode':
                    hospital_geo['geocoded_hospital_address'][i]
            })
        # Add to count for successes
        added_geo = added_geo + 1
    except Exception:
        # Append the new failed row to a dataframe of omissions
        new_row = {'hospital_pk': str(hospital_geo['hospital_pk'][i]),
                   'fips_code': str(hospital_geo['fips_code'][i]),
                   'geocoded_hospital_address':
                       str(hospital_geo['geocoded_hospital_address'][i])
                   }
        omitted_geo_df.loc[len(omitted_geo_df)] = new_row
        # Export the omission dataframe to a csv
        omitted_geo_df.to_csv('omitted/omitted_geo.csv', index=False)
        # Add to count for failures
        skipped_geo = skipped_geo + 1

# Update 'NaN' string to be a NULL value
cur.execute(
    """
        UPDATE Seymour_geo
        SET hosptial_geocode = NULL
        WHERE hosptial_geocode = 'NaN'
    """
)

# Update 'NA' string to be a NULL value
cur.execute(
    """
        UPDATE Seymour_geo
        SET hosptial_geocode = NULL
        WHERE hosptial_geocode = 'NA'
    """
)

# Close SQL
conn.commit()
conn.close()

# Print duplicates dropped
print(f"Dropped {duplicates_dropped_weekly} duplicate rows of" +
      " hospital_pk/date pairings before inserting into Seymour_weekly_info.")
print(f"Dropped {duplicates_dropped_geo} duplicate rows of" +
      " hospital_pk before inserting into Seymour_weekly_geo.")
print("")
# Print number of rows successfully inserted
print(f"Successfully inserted {added_weekly}" +
      " rows to Seymour_weekly_info.")
print(f"Successfully inserted {added_geo}" +
      " rows to Seymour_geo.")
print("")
# Print number of rows that raised errors
print(f"{skipped_weekly} rows were not inserted" +
      " into Seymour_weekly_info due to errors.")
print(f"{skipped_geo} rows were not inserted" +
      " into Seymour_geo due to errors.")
print("")
