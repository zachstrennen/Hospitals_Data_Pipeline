import pandas as pd
import credentials
import psycopg2
import numpy as np
import plotly.express as px
import sys
from datetime import timedelta

# Store filename from commandline
file_name = "reports/" + sys.argv[1]

# Access date
end_date = sys.argv[2]

# Connect to server
conn = psycopg2.connect(
    host="pinniped.postgres.database.azure.com", dbname=credentials.get_db(),
    user=credentials.get_username(), password=credentials.get_password()
)
cur = conn.cursor()

# Get weekly info as dataframe
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

# Get geo info as dataframe
cur.execute("SELECT * FROM Seymour_geo")
sql_geo = pd.DataFrame(cur.fetchall(), columns=[
    "hospital_pk",
    "fips_code",
    "hosptial_geocode"
])

# Get Hospital GI as dataframe
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

# Get quality info as dataframe
cur.execute("SELECT * FROM Seymour_quality")
sql_quality = pd.DataFrame(cur.fetchall(), columns=[
    "Facility ID",
    "date",
    "Hospital overall rating"
])

# Close server connection
conn.commit()
conn.close()

# PLOT 1

# Convert 'collection_week' and 'date' to datetime
sql_weekly['collection_week'] = pd.to_datetime(
    sql_weekly['collection_week']
).dt.date

# Get time range to look at
end_date = pd.to_datetime(end_date)
end_date = end_date.date()
start_date = end_date - timedelta(days=29)

# Filter data to time range
sql_weekly = sql_weekly[
    (sql_weekly['collection_week'] >= start_date) &
    (sql_weekly['collection_week'] <= end_date)
]

# Convert date back to regular format
sql_weekly['collection_week'] = pd.to_datetime(sql_weekly['collection_week'])

# Check that there is data to visualize
if sql_weekly.shape[0] == 0:
    print("No data associated with given week!")
    sys.exit()

# Merge weekly and quality
merged_data = pd.merge(sql_weekly, sql_quality,
                       left_on='hospital_pk',
                       right_on='Facility ID',
                       how='left')

# Set collection week to be the index
weekly_record_counts = sql_weekly['collection_week']\
    .value_counts().sort_index()
weekly_record_counts.index = weekly_record_counts.index.strftime('%Y-%m-%d')


def generate_visualization1():
    # Plotting with Plotly Express
    fig = px.bar(weekly_record_counts,
                 x=weekly_record_counts.index,
                 y=weekly_record_counts.values,
                 labels={'y': 'Number of Records'},
                 title='Number of Hospital Records Loaded Each Week')

    # Annotating each bar
    fig.update_traces(text=weekly_record_counts.values, textposition='inside')

    # Formatting dates on the x-axis
    fig.update_xaxes(type='category', tickangle=0, tickformat='%Y-%m-%d')

    # Change axis title
    fig.update_layout(xaxis_title='Collection Week')

    return fig


# Store plot for html
interactive_html1 = generate_visualization1()
html_content1 = interactive_html1.to_html(full_html=False)

# TABLE 1

# Group by week and summarize bed information
beds_summary1 = sql_weekly.groupby('collection_week').agg({
    'all_adult_hospital_beds_7_day_avg': 'sum',
    'all_pediatric_inpatient_beds_7_day_avg': 'sum',
    'all_adult_hospital_inpatient_bed_occupied_7_day_coverage': 'sum'
}).tail(5)  # Last 5 weeks
beds_summary2 = sql_weekly.groupby('collection_week').agg({
    'all_pediatric_inpatient_bed_occupied_7_day_avg': 'sum',
    'total_icu_beds_7_day_avg': 'sum',
    'icu_beds_used_7_day_avg': 'sum'
}).tail(5)  # Last 5 weeks
beds_summary3 = sql_weekly.groupby('collection_week').agg({
    'inpatient_beds_used_covid_7_day_avg': 'sum',
    'staffed_icu_adult_patients_confirmed_covid_7_day_avg': 'sum'
}).tail(5)  # Last 5 weeks

# Store tables for html
interactive_html2 = beds_summary1.to_html(classes='table table-sm',
                                          justify='center')
interactive_html3 = beds_summary2.to_html(classes='table table-sm',
                                          justify='center')
# 9 instead of 4 because third table added later
interactive_html9 = beds_summary3.to_html(classes='table table-sm',
                                          justify='center')

# PLOT 2

# Rename columns for visualization output
merged_data['Average total ICU beds weekly'] =\
    merged_data['total_icu_beds_7_day_avg']
merged_data['Average used ICU beds weekly'] =\
    merged_data['icu_beds_used_7_day_avg']

# Assuming the quality rating is available and properly aligned
# with the weekly data
bed_usage_by_rating = merged_data.groupby('Hospital overall rating').agg({
    'Average total ICU beds weekly': 'mean',
    'Average used ICU beds weekly': 'mean'
})

barWidth = 0.35
r1 = np.arange(len(bed_usage_by_rating))
r2 = [x + barWidth for x in r1]


def generate_visualization2():
    # Plotting with Plotly Express
    fig = px.bar(bed_usage_by_rating,
                 x=bed_usage_by_rating.index,
                 y=[
                     'Average total ICU beds weekly',
                     'Average used ICU beds weekly'
                 ],
                 labels={'value': 'Average Beds', 'variable': 'Bed Type'},
                 title='Comparison of Total ICU Beds and '
                       'ICU Beds Used by Hospital Rating')

    #  Additional plot specifications
    fig.update_layout(barmode='group',
                      xaxis_title='Hospital Quality Rating',
                      yaxis_title='Average Beds')

    return fig


# Store plot for html
interactive_html4 = generate_visualization2()
html_content2 = interactive_html4.to_html(full_html=False)

# PLOT 3


def generate_visualization3():
    # Group by week and aggregate
    sql_weekly2 = sql_weekly
    sql_weekly2['Average hospital beds for 7 days'] =\
        sql_weekly2[
            'all_adult_hospital_inpatient_bed_occupied_7_day_coverage'
        ]
    sql_weekly2['Average hospital covid beds for 7 days'] =\
        sql_weekly2['inpatient_beds_used_covid_7_day_avg']

    # Aggregate bed typed for collection week
    total_beds_used = sql_weekly.groupby('collection_week').agg({
        'Average hospital beds for 7 days': 'sum',
        'Average hospital covid beds for 7 days': 'sum'
    })
    total_beds_used.index = pd.to_datetime(total_beds_used.index)

    # Plotting with Plotly
    fig = px.line(total_beds_used,
                  x=total_beds_used.index,
                  y=['Average hospital beds for 7 days',
                     'Average hospital covid beds for 7 days'],
                  labels={'value': 'Number of Beds Used',
                          'variable': 'Bed Type'},
                  title='Total Number of Hospital Beds Used Per Week',
                  line_shape='linear', render_mode='svg')

    #  Additional plot specifications
    fig.update_layout(xaxis_title='Collection Week', legend_title='Bed Type')

    return fig


# Store plot for html
interactive_html5 = generate_visualization3()
html_content3 = interactive_html5.to_html(full_html=False)

# PLOT 4

# Merging 'sql_weekly' with 'sql_hospital' on 'hospital_pk' or 'Facility ID'
weekly_hospital_merged = pd.merge(sql_weekly,
                                  sql_hospital,
                                  left_on='hospital_pk',
                                  right_on='Facility ID',
                                  how='inner')

# Make sure values are not null
weekly_hospital_merged =\
    weekly_hospital_merged[weekly_hospital_merged[
                               'all_adult_hospital_beds_7_day_avg'
                           ].notnull()
                           & (weekly_hospital_merged[
                                  'all_adult_hospital_beds_7_day_avg'
                              ] != 0)]


# Grouping by state to find the total and average hospital beds and covid cases
state_summary = weekly_hospital_merged.groupby('State').agg(
    total_beds=pd.NamedAgg(column='all_adult_hospital_beds_7_day_avg',
                           aggfunc='sum'),
    avg_beds=pd.NamedAgg(column='all_adult_hospital_beds_7_day_avg',
                         aggfunc='mean'),
    total_covid_cases=pd.NamedAgg(column='inpatient_beds_used_covid_7_day_avg',
                                  aggfunc='sum'),
    avg_covid_cases=pd.NamedAgg(column='inpatient_beds_used_covid_7_day_avg',
                                aggfunc='mean')
).reset_index()

# Calculating the percentage of beds used for COVID in each state
state_summary['percent_beds_used_for_covid'] =\
    (state_summary['total_covid_cases'] / state_summary['total_beds']) * 100

# Sorting states by the fewest open beds
# (highest percentage of beds used for COVID)
states_fewest_open_beds =\
    state_summary.sort_values('percent_beds_used_for_covid',
                              ascending=False)


def generate_visualization4():
    # Plotting with Plotly
    fig = px.bar(states_fewest_open_beds, x='State',
                 y='percent_beds_used_for_covid',
                 labels={'percent_beds_used_for_covid':
                         'Percentage of Beds Used for COVID'},
                 title='COVID Hospitalization Rate by State')

    #  Additional plot specifications
    fig.update_layout(xaxis_title='State',
                      yaxis_title='Percentage of Beds Used for COVID')

    return fig


# Store plot for html
interactive_html6 = generate_visualization4()
html_content4 = interactive_html6.to_html(full_html=False)


# PLOT 5

# Merge 'sql_weekly' and 'sql_hospital' datasets
weekly_hospital_type_merged = pd.merge(sql_weekly,
                                       sql_hospital,
                                       left_on='hospital_pk',
                                       right_on='Facility ID',
                                       how='inner')

# Calculate hospital utilization percentage
# (percentage of available beds being used)
weekly_hospital_type_merged['hospital_utilization'] = (
    weekly_hospital_type_merged[
        'all_adult_hospital_inpatient_bed_occupied_7_day_coverage'
    ] /
    weekly_hospital_type_merged['all_adult_hospital_beds_7_day_avg']
) * 100

# Handle missing or infinite values in utilization calculation
weekly_hospital_type_merged['hospital_utilization'] =\
    weekly_hospital_type_merged['hospital_utilization'].replace([
        np.inf, -np.inf
    ], np.nan)

# Convert 'collection_week' to datetime for time series plotting
weekly_hospital_type_merged['collection_week'] =\
    pd.to_datetime(weekly_hospital_type_merged['collection_week'])


def generate_visualization5():
    mean_values = weekly_hospital_type_merged.groupby(
        ['collection_week', 'Hospital Type']
    )['hospital_utilization'].mean().reset_index()
    # Plotting with Plotly Express
    fig = px.line(mean_values,
                  x='collection_week',
                  y='hospital_utilization',
                  color='Hospital Type',
                  labels={'hospital_utilization': 'Hospital Utilization (%)'},
                  title='Hospital Utilization by Type of Hospital Over Time')

    # Rotate the x-axis labels for better readability
    fig.update_xaxes(tickangle=45)

    # Positioning the legend in the top right corner outside of the plot
    fig.update_layout(legend_title='Hospital Type',
                      legend=dict(x=1, y=1, traceorder='normal'))

    # Update x-axis title
    fig.update_layout(xaxis_title='Collection Week')

    return fig


# Store plot for html
interactive_html7 = generate_visualization5()
html_content5 = interactive_html7.to_html(full_html=False)

# TABLE 2

# Merge 'sql_weekly' and 'sql_hospital' datasets
weekly_hospital_merged = pd.merge(sql_weekly,
                                  sql_hospital,
                                  left_on='hospital_pk',
                                  right_on='Facility ID',
                                  how='inner')

# Sort the data by state and collection week,
# then calculate the weekly change in COVID cases
weekly_hospital_merged.sort_values(by=['State', 'collection_week'],
                                   inplace=True)
weekly_hospital_merged['covid_cases_change'] =\
    weekly_hospital_merged.groupby('State')[
        'inpatient_beds_used_covid_7_day_avg'
    ].diff()

# Round the change in cases to ensure they are integers
weekly_hospital_merged['covid_cases_change'] =\
    weekly_hospital_merged['covid_cases_change'].round()

# Drop NaN values that result from the diff operation
# (the first value in each group will be NaN)
weekly_hospital_merged.dropna(subset=['covid_cases_change'],
                              inplace=True)

# Calculate the total increase in cases for each state
total_increase_by_state =\
    weekly_hospital_merged.groupby('State')[
        'covid_cases_change'
    ].sum().reset_index()

# Sort the states by the total increase in cases
total_increase_by_state =\
    total_increase_by_state.sort_values(by='covid_cases_change',
                                        ascending=False)

# Display the top 10 states with the highest increase in cases
top_10_states = total_increase_by_state.head(10)

# Convert table to html
interactive_html8 = top_10_states.to_html(classes='table table-sm',
                                          justify='center')

# Establish necessary headers for html file
header = """
<!DOCTYPE html>
<html>
<head>
    <title>Hospitals Report</title>
</head>
<body>
    <h1>Summary of Beds by Collection Week</h1>
"""

header2 = """
    <h1>Top Ten States with the Highest Increase Cases</h1>
"""

footer = """
</body>
</html>
"""

# Print all stored plots and tables to an html file in reports directory
# Note: These are not in numerical order because print order was adjusted
# at the end of building
with open(file_name, 'w') as f:
    f.write(header)
    f.write(interactive_html2)
    f.write(interactive_html3)
    f.write(interactive_html9)
    f.write(header2)
    f.write(interactive_html8)
    f.write(html_content1)
    f.write(html_content2)
    f.write(html_content3)
    f.write(html_content4)
    f.write(html_content5)
    f.write(footer)
