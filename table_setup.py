import credentials
# install package psycopg2-binary
import psycopg2

# Connect to SQL
conn = psycopg2.connect(
    host=credentials.get_hostname(), dbname=credentials.get_db(),
    user=credentials.get_username(), password=credentials.get_password()
)
cur = conn.cursor()

# Each row represents a single hospital and all associated information
cur.execute(
    """
        CREATE TABLE Seymour_hospital (
            hospital_pk VARCHAR UNIQUE,
            hospital_name VARCHAR,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            zip_code INT,
            county VARCHAR,
            hospital_type VARCHAR,
            emergency_services BOOL
        )
    """
    )

# Each row represents data for a given hospital during a given week
cur.execute(
    """
        CREATE TABLE Seymour_weekly_info (
            -- refers to the unique IDs in the Seymour_hospital table
            hospital_pk VARCHAR,
            collection_week DATE,
            all_adult_hospital_beds_7_day_avg FLOAT,
            all_pediatric_inpatient_beds_7_day_avg FLOAT,
            all_adult_hospital_inpatient_bed_occupied_7_day_coverage FLOAT,
            all_pediatric_inpatient_bed_occupied_7_day_avg FLOAT,
            total_icu_beds_7_day_avg FLOAT,
            icu_beds_used_7_day_avg FLOAT,
            inpatient_beds_used_covid_7_day_avg FLOAT,
            staffed_icu_adult_patients_confirmed_covid_7_day_avg FLOAT
        )
    """
    )

# Each row represents the quality of a given hospital for at a given time
cur.execute(
    """
        CREATE TABLE Seymour_quality(
            -- refers to the unique IDs in the Seymour_hospital table
            hospital_pk VARCHAR,
            date DATE,
            rating INT CHECK (-1 <= rating and rating <= 5)
        )
    """
    )

# Each row represents a hospital's fips_code and geocode
# This table exist because of the nature of the data
cur.execute(
    """
        CREATE TABLE Seymour_geo(
            -- refers to the unique IDs in the Seymour_hospital table
            hospital_pk VARCHAR,
            fips_code FLOAT,
            hosptial_geocode VARCHAR
        )
    """
    )

# Close SQL
conn.commit()
conn.close()
