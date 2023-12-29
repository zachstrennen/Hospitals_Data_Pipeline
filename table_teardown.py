import credentials
# install package psycopg2-binary
import psycopg2

# Connect to SQL
conn = psycopg2.connect(
    host=credentials.get_hostname(), dbname=credentials.get_db(),
    user=credentials.get_username(), password=credentials.get_password()
)

cur = conn.cursor()

cur.execute("DROP TABLE Seymour_hospital")

cur.execute("DROP TABLE Seymour_weekly_info")

cur.execute("DROP TABLE Seymour_quality")

cur.execute("DROP TABLE Seymour_geo")

# Close SQL
conn.commit()
conn.close()
