
import psycopg2
import streamlit as st
import pandas as pd

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="rootpassword",
    host="database-1.ch4oq6k2otmz.ap-south-1.rds.amazonaws.com",
    port="5432"
)

# 1.Define the SQL query to calculate the total population for each district
query = """
    SELECT "District",
           SUM("Population") AS total_population
    FROM census_data
    GROUP BY "District"
    ORDER BY "District";
"""

# Execute the query
cur = conn.cursor()
cur.execute(query)

# Fetch the results and convert them into a DataFrame
results = cur.fetchall()
columns = ['District', 'Total Population']
df = pd.DataFrame(results, columns=columns)

# Display the results in Streamlit
st.title("Census Data Standardization and Analysis Pipeline")
st.write("1. Total Population of Each District:")
st.write(df)

# 2. Define the SQL query to calculate the number of literate males and females for each district
query = """
    SELECT "District",
           SUM("Literate_Male") AS total_literate_males,
           SUM("Literate_Female") AS total_literate_females
    FROM census_data
    GROUP BY "District"
    ORDER BY "District";
"""

# Execute the query
cur = conn.cursor()
cur.execute(query)

# Fetch the results and convert them into a DataFrame
results = cur.fetchall()
columns = ['District', 'Total Literate Males', 'Total Literate Females']
df = pd.DataFrame(results, columns=columns)

# Display the results in Streamlit
st.write("2. Number of Literate Males and Females in Each District:")
st.write(df)

# 3. Define the SQL query to calculate the total number of workers (male and female) and total population for each district
query = """
    SELECT "District",
           SUM("Workers") AS total_workers,
           SUM("Population") AS total_population
    FROM census_data
    GROUP BY "District"
    ORDER BY "District";
"""

# Execute the query
cur = conn.cursor()
cur.execute(query)

# Fetch the results and convert them into a DataFrame
results = cur.fetchall()
columns = ['District', 'Total Workers', 'Total Population']
df = pd.DataFrame(results, columns=columns)

# Calculate the percentage of workers (both male and female) for each district
df['Percentage of Workers'] = (df['Total Workers'] / df['Total Population']) * 100

# Display the results in Streamlit
st.write("3. Percentage of Workers (Both Male and Female) in Each District:")
st.write(df)

# 4. Define the SQL query to calculate the number of households with access to LPG or PNG as a cooking fuel for each district
query = """
    SELECT "District",
           SUM("LPG_or_PNG_Households") AS lpg_or_png_households
    FROM census_data
    GROUP BY "District"
    ORDER BY "District";
"""

# Execute the query
cur = conn.cursor()
cur.execute(query)

# Fetch the results and convert them into a DataFrame
results = cur.fetchall()
columns = ['District', 'Households with LPG or PNG']
df = pd.DataFrame(results, columns=columns)

# Display the results in Streamlit
st.write("4. Number of Households with Access to LPG or PNG as Cooking Fuel in Each District:")
st.write(df)

# 5. Define the SQL query to calculate the religious composition of each district
query = """
    SELECT "District",
           "Hindus",
           "Muslims",
           "Christians",
           "Sikhs",
           "Buddhists",
           "Jains",
           "Others_Religions",
           "Religion_Not_Stated"
    FROM census_data
    ORDER BY "District";
"""

# Execute the query
cur = conn.cursor()
cur.execute(query)

# Fetch the results and convert them into a DataFrame
results = cur.fetchall()
columns = ['District', 'Hindus', 'Muslims', 'Christians', 'Sikhs', 'Buddhists', 'Jains', 'Others', 'Religion Not Stated']
df = pd.DataFrame(results, columns=columns)

# Display the results in Streamlit
st.write("5. Religious Composition of Each District:")
st.write(df)

# 6. # SQL query to find the number of households with internet access in each district
query = """
    SELECT 
        "District",
        SUM("Households_with_Internet") AS "Households_with_Internet"
    FROM 
        census_data
    GROUP BY 
        "District";
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("6. Number of households with internet access in each district:")
st.write(result)

# 7. # Execute the SQL query
query = """
    SELECT 
        "District",
        SUM("Below_Primary_Education") AS Below_Primary,
        SUM("Primary_Education") AS Primary,
        SUM("Middle_Education") AS Middle,
        SUM("Secondary_Education") AS Secondary,
        SUM("Higher_Education") AS Higher,
        SUM("Graduate_Education") AS Graduate,
        SUM("Other_Education") AS Other,
        SUM("Literate_Education") AS Literate,
        SUM("Illiterate_Education") AS Illiterate
    FROM 
        census_data
    GROUP BY 
        "District";
"""

result = pd.read_sql_query(query, conn)

# Display the result in Streamlit
st.write("7.Educational attainment distribution in each district:")
st.write(result)

# 8. SQL query to find the number of households with access to various modes of transportation in each district
query = """
    SELECT 
        "District",
        "Households_with_Bicycle",
        "Households_with_Car_Jeep_Van",
        "Households_with_Radio_Transistor",
        "Households_with_Television",
        "Households_with_Scooter_Motorcycle_Moped"
    FROM 
        census_data;
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("8. Number of households with access to various modes of transportation in each district:")
st.write(result)

# 9. SQL query to find the condition of occupied census houses in each district
query = """
    SELECT 
        "District",
        "Condition_of_occupied_census_houses_Dilapidated_Households",
        "Households_with_separate_kitchen_Cooking_inside_house",
        "Having_bathing_facility_Total_Households",
        "Having_latrine_facility_within_the_premises_Total_Households"
    FROM 
        census_data;
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("9. Condition of occupied census houses in each district:")
st.write(result)

# 10. SQL query to find the distribution of household sizes in each district
query = """
    SELECT 
        "District",
        "Household_size_1_person_Households",
        "Household_size_2_persons_Households",
        "Household_size_1_to_2_persons",
        "Household_size_3_persons_Households",
        "Household_size_3_to_5_persons_Households",
        "Household_size_4_persons_Households",
        "Household_size_5_persons_Households",
        "Household_size_6_8_persons_Households",
        "Household_size_9_persons_and_above_Households"
    FROM 
        census_data;
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("10. Distribution of household sizes in each district:")
st.write(result)

# 11. SQL query to find the total number of households in each state
query = """
    SELECT 
        "State/UT" as State,
        SUM("Households") as Total_Households
    FROM 
        census_data
    GROUP BY 
        "State/UT";
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("11. Total number of households in each state:")
st.write(result)

# 12. # SQL query to find the number of households with a latrine facility within the premises in each state
query = """
    SELECT 
        "State/UT" as State,
        SUM("Having_latrine_facility_within_the_premises_Total_Households") as Households_with_Latrine_Facility
    FROM 
        census_data
    GROUP BY 
        "State/UT";
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("12. Number of households with a latrine facility within the premises in each state:")
st.write(result)

# 13. # SQL query to find the average household size in each state
query = """
    SELECT 
        "State/UT" as State,
        AVG(
            "Household_size_1_person_Households" + 
            "Household_size_2_persons_Households" + 
            "Household_size_1_to_2_persons" + 
            "Household_size_3_persons_Households" + 
            "Household_size_3_to_5_persons_Households" + 
            "Household_size_4_persons_Households" + 
            "Household_size_5_persons_Households" + 
            "Household_size_6_8_persons_Households" + 
            "Household_size_9_persons_and_above_Households"
        ) as Average_Household_Size
    FROM 
        census_data
    GROUP BY 
        "State/UT";
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("13. Average household size in each state:")
st.write(result)

# 14. SQL query to find the number of households owned versus rented in each state
query = """
    SELECT 
        "State/UT" as State,
        SUM("Ownership_Owned_Households") as Owned_Households,
        SUM("Ownership_Rented_Households") as Rented_Households
    FROM 
        census_data
    GROUP BY 
        "State/UT";
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("14. Number of households owned versus rented in each state:")
st.write(result)

# 15. SQL query to find the distribution of different types of latrine facilities in each state
query = """
    SELECT 
        "State/UT" as State,
        SUM("Type_of_latrine_facility_Pit_latrine_Households") as Pit_Latrine,
        SUM("Type_of_latrine_facility_Other_latrine_Households") as Other_Latrine,
        SUM("Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households") as Night_Soil_Disposed,
        SUM("Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households") as Flush_Latrine
    FROM 
        census_data
    GROUP BY 
        "State/UT";
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("15. Distribution of different types of latrine facilities in each state:")
st.write(result)

# 16. SQL query to find the number of households with access to drinking water sources near the premises in each state
query = """
    SELECT 
        "State/UT" as State,
        SUM("Main_source_of_drinking_water_Un_covered_well_Households") as Uncovered_Well,
        SUM("Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households") as Handpump_Tubewell_Borewell,
        SUM("Main_source_of_drinking_water_Spring_Households") as Spring,
        SUM("Main_source_of_drinking_water_River_Canal_Households") as River_Canal,
        SUM("Main_source_of_drinking_water_Other_sources_Households") as Other_Sources
    FROM 
        census_data
    GROUP BY 
        "State/UT";
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("16. Number of households with access to drinking water sources near the premises in each state:")
st.write(result)

# 17. SQL query to find the average household income distribution in each state based on the power parity categories
query = """
    SELECT 
        "State/UT" as State,
        AVG("Power_Parity_Less_than_Rs_45000") as Less_than_Rs_45000,
        AVG("Power_Parity_Rs_45000_90000") as Rs_45000_90000,
        AVG("Power_Parity_Rs_90000_150000") as Rs_90000_150000,
        AVG("Power_Parity_Rs_45000_150000") as Rs_45000_150000,
        AVG("Power_Parity_Rs_150000_240000") as Rs_150000_240000,
        AVG("Power_Parity_Rs_240000_330000") as Rs_240000_330000,
        AVG("Power_Parity_Rs_150000_330000") as Rs_150000_330000,
        AVG("Power_Parity_Rs_330000_425000") as Rs_330000_425000,
        AVG("Power_Parity_Rs_425000_545000") as Rs_425000_545000,
        AVG("Power_Parity_Rs_330000_545000") as Rs_330000_545000,
        AVG("Power_Parity_Above_Rs_545000") as Above_Rs_545000
    FROM 
        census_data
    GROUP BY 
        "State/UT";
"""

# Execute the SQL query and convert the result to a DataFrame
result = pd.read_sql_query(query, conn)

# Display the DataFrame in Streamlit
st.write("17. Average household income distribution in each state based on power parity categories:")
st.write(result)


# 18. Execute SQL query and convert to DataFrame
query = """
SELECT 
    "State/UT",
    "Married_couples_1_Households" / "Married_couples_None_Households" AS Percent_1_Person,
    "Married_couples_2_Households" / "Married_couples_None_Households" AS Percent_2_Persons,
    "Married_couples_3_Households" / "Married_couples_None_Households" AS Percent_3_Persons,
    "Married_couples_3_or_more_Households" / "Married_couples_None_Households" AS Percent_3_Or_More_Persons
FROM 
    census_data;
"""
df = pd.read_sql_query(query, conn)

# Display the DataFrame using Streamlit
st.write("18. Percentage of married couples with different household sizes in each state:")
st.write(df)

# Execute SQL query and convert to DataFrame
query = """
SELECT 
    "State/UT",
    "Power_Parity_Less_than_Rs_45000" AS Below_45000,
    "Power_Parity_Rs_45000_90000" AS Rs_45000_90000,
    "Power_Parity_Rs_90000_150000" AS Rs_90000_150000,
    "Power_Parity_Rs_45000_150000" AS Rs_45000_150000,
    "Power_Parity_Rs_150000_240000" AS Rs_150000_240000,
    "Power_Parity_Rs_240000_330000" AS Rs_240000_330000,
    "Power_Parity_Rs_150000_330000" AS Rs_150000_330000,
    "Power_Parity_Rs_330000_425000" AS Rs_330000_425000,
    "Power_Parity_Rs_425000_545000" AS Rs_425000_545000,
    "Power_Parity_Rs_330000_545000" AS Rs_330000_545000,
    "Power_Parity_Above_Rs_545000" AS Above_545000,
    "Total_Power_Parity" AS Total
FROM 
    census_data;
"""
df = pd.read_sql_query(query, conn)

# Display the DataFrame using Streamlit
st.write("19. Households which falls below the poverty line in each state based on the power parity categories:")
st.write(df)

# 20.Define the SQL query to calculate the overall literacy rate for each state
query = """
    SELECT "State/UT",
           (SUM("Literate") * 100.0 / SUM("Population")) AS literacy_rate
    FROM census_data
    GROUP BY "State/UT"
    ORDER BY "State/UT";
"""

# Execute the query
cur = conn.cursor()
cur.execute(query)

# Fetch the results and convert them into a DataFrame
results = cur.fetchall()
columns = ['State/UT', 'Literacy Rate']
df = pd.DataFrame(results, columns=columns)

# Display the results in Streamlit
st.write("20. Overall Literacy Rate in Each State:")
st.write(df)

# Close the cursor and connection
cur.close()
conn.close()