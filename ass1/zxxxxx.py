#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# Third-party libraries
# NOTE: You may **only** use the following third-party libraries:
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd 
from thefuzz import fuzz
from thefuzz import process
# NOTE: It isn't necessary to use all of these to complete the assignment, 
# but you are free to do so, should you choose.

# Standard libraries
# NOTE: You may use **any** of the Python 3.11 or Python 3.12 standard libraries:
# https://docs.python.org/3.11/library/index.html
# https://docs.python.org/3.12/library/index.html
from pathlib import Path
# ... import your standard libraries here ...
import os


######################################################
# NOTE: DO NOT MODIFY THE LINE BELOW ...
######################################################
studentid = Path(__file__).stem

######################################################
# NOTE: DO NOT MODIFY THE FUNCTION BELOW ...
######################################################
def log(question, output_df, other):
    print(f"--------------- {question}----------------")

    if other is not None:
        print(question, other)
    if output_df is not None:
        df = output_df.head(5).copy(True)
        for c in df.columns:
            df[c] = df[c].apply(lambda a: a[:20] if isinstance(a, str) else a)

        df.columns = [a[:10] + "..." for a in df.columns]
        print(df.to_string())


######################################################
# NOTE: YOU MAY ADD ANY HELPER FUNCTIONS BELOW ...
######################################################



######################################################
# QUESTIONS TO COMPLETE BELOW ...
######################################################

######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_1(jobs_csv):
    """Read the data science jobs CSV file into a DataFrame.

    See the assignment spec for more details.

    Args:
        jobs_csv (str): Path to the jobs CSV file.

    Returns:
        DataFrame: The jobs DataFrame.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    df = pd.read_csv('ds_jobs.csv')

    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 1", output_df=df, other=df.shape)
    return df



######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_2(cost_csv, cost_url):
    """Read the cost of living CSV into a DataFrame.  If the CSV file does not 
    exist, scrape it from the specified URL and save it to the CSV file.

    See the assignment spec for more details.

    Args:
        cost_csv (str): Path to the cost of living CSV file.
        cost_url (str): URL of the cost of living page.

    Returns:
        DataFrame: The cost of living DataFrame.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    if not os.path.exists(cost_csv):
        dfs = pd.read_html(cost_url)
        df = dfs[0]
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]
        df.to_csv(cost_csv, index=False)
    else:
        df = pd.read_csv(cost_csv)

    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 2", output_df=df, other=df.shape)
    return df


######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_3(currency_csv, currency_url):
    """Read the currency conversion rates CSV into a DataFrame.  If the CSV 
    file does not exist, scrape it from the specified URL and save it to 
    the CSV file.

    See the assignment spec for more details.

    Args:
        cost_csv (str): Path to the currency conversion rates CSV file.
        cost_url (str): URL of the currency conversion rates page.

    Returns:
        DataFrame: The currency conversion rates DataFrame.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    if not os.path.exists(currency_csv):
        df_list = pd.read_html(currency_url)
        df = df_list[0]

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)
        df.columns = df.columns.str.replace('\xa0', ' ', regex=True)
        column_remove = '30 Jun 23'
        if column_remove in df.columns:
            df.drop(columns=[column_remove], inplace=True)
        column_rename = '31 Dec 23'
        if column_rename in df.columns:
            df.rename(columns={column_rename: 'rate'}, inplace=True)
        
        df.columns = df.columns.str.lower()
        df.to_csv(currency_csv, index=False)
    else:
        df = pd.read_csv(currency_csv)

    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 3", output_df=df, other=df.shape)
    return df

######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_4(country_csv, country_url):
    """Read the country codes CSV into a DataFrame.  If the CSV file does not 
    exist, it will be scrape the data from the specified URL and save it to the 
    CSV file.

    See the assignment spec for more details.

    Args:
        cost_csv (str): Path to the country codes CSV file.
        cost_url (str): URL of the country codes page.

    Returns:
        DataFrame: The country codes DataFrame.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    if not os.path.exists(country_csv):
        df_list = pd.read_html(country_url)
        df = df_list[0]
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)

        df.columns = df.columns.str.replace('\xa0', ' ', regex=True)
        df.rename(columns=lambda x: x.strip().lower().replace(' ', '_'), inplace=True)
        columns_remove = ['year', 'cctld', 'notes']
        df.drop(columns=columns_remove, inplace=True)

        df.rename(columns={'country_name_(using_title_case)': 'country'}, inplace=True)
        df['code'] = df['code'].str.lower()
        df.to_csv(country_csv, index=False)

    else:
        df = pd.read_csv(country_csv)

    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 4", output_df=df, other=df.shape)
    return df


######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_5(jobs_df):
    """Summarise some dimensions of the jobs DataFrame.

    See the assignment spec for more details.

    Args:
        jobs_df (DataFrame): The jobs DataFrame returned in question 1.

    Returns:
        DataFrame: The summary DataFrame.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    df = pd.DataFrame(columns=['observations', 'distinct', 'missing'])
    for column in jobs_df.columns:
        observations = jobs_df[column].notnull().sum()
        distinct = jobs_df[column].nunique()
        missing = jobs_df[column].isnull().sum()
        df.loc[column] = [observations, distinct, missing]
    
    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 5", output_df=df, other=df.shape)
    return df


######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_6(jobs_df):
    """Add an experience rating column to the jobs DataFrame.

    See the assignment spec for more details.

    Args:
        jobs_df (DataFrame): The jobs DataFrame returned in question 1.

    Returns:
        DataFrame: The jobs DataFrame with the experience rating column added.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    df=jobs_df.copy()
    rating = {
        'EN': 1,
        'MI': 2,
        'SE': 3,
        'EX': 4
    }
    
    df['experience_rating'] = df['experience_level'].map(rating)

    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 6", output_df=df, other=df.shape)
    return df


######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_7(jobs_df, country_df):
    """Merge the jobs and country codes DataFrames.

    See the assignment spec for more details.

    Args:
        jobs_df (DataFrame): The jobs DataFrame returned in question 6.
        country_df (DataFrame): The country codes DataFrame returned in 
                                question 4.

    Returns:
        DataFrame: The merged DataFrame.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    df=jobs_df.copy()
    df['employee_residence'] = df['employee_residence'].str.lower()
    df = df.merge(country_df, how='left', left_on='employee_residence', right_on='code')
    df.rename(columns={'country': 'country'}, inplace=True)
    df.drop(columns='code', inplace=True)
    
    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 7", output_df=df, other=df.shape)
    return df


######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_8(jobs_df, currency_df):
    """Add an Australian dollar salary column to the jobs DataFrame.

    See the assignment spec for more details.

    Args:
        jobs_df (DataFrame): The jobs DataFrame returned in question 7.
        currency_df (DataFrame): The currency conversion rates DataFrame 
                                 returned in question 3.

    Returns:
        DataFrame: The jobs DataFrame with the Australian dollar salary column
                   added.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    # Filter to only consider the work year 2023
    df = jobs_df[jobs_df['work_year'] == 2023].copy()
    currency_df['rate'] = pd.to_numeric(currency_df['rate'], errors='coerce')
    rate = currency_df[currency_df['currency'].str.contains('United States', case=False)]['rate'].iloc[0]
    df['salary_in_aud'] = (df['salary_in_usd'] * rate).astype(int)

    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 8", output_df=df, other=df.shape)
    return df


######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_9(cost_df):
    """Re-scale the cost of living DataFrame to be relative to Australia.

    See the assignment spec for more details.

    Args:
        cost_df (DataFrame): The cost of living DataFrame returned in question 2.

    Returns:
        DataFrame: The re-scaled cost of living DataFrame.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    df = cost_df[['country', 'cost_of_living_plus_rent_index']].copy()
    au_index = df[df['country'] == 'Australia']['cost_of_living_plus_rent_index'].iloc[0]
    df['cost_of_living_plus_rent_index'] = df['cost_of_living_plus_rent_index'] / au_index * 100
    df['cost_of_living_plus_rent_index'] = df['cost_of_living_plus_rent_index'].round(1)
    df = df.sort_values('cost_of_living_plus_rent_index')
    
    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 9", output_df=df, other=df.shape)
    return df


######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_10(jobs_df, cost_df):
    """Merge the jobs and cost of living DataFrames.

    See the assignment spec for more details.

    Args:
        jobs_df (DataFrame): The jobs DataFrame returned in question 8.
        cost_df (DataFrame): The cost of living DataFrame returned in question 9.

    Returns:
        DataFrame: The merged DataFrame.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    cost_dict = cost_df.set_index('country')['cost_of_living_plus_rent_index'].to_dict()
    col_values = []

    for country in jobs_df['country']:
        match = process.extractOne(country, cost_dict.keys(), score_cutoff=90)
        if match:
            cost_of_living = cost_dict[match[0]]
        else:
            cost_of_living = None
        
        col_values.append(cost_of_living)

    jobs_df['cost_of_living'] = col_values
    df = jobs_df.dropna(subset=['cost_of_living'])
    
    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 10", output_df=df, other=df.shape)
    return df


######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_11(jobs_df):
    """Create a pivot table of the average salary in AUD by country and 
    experience rating.

    See the assignment spec for more details.

    Args:
        jobs_df (DataFrame): The jobs DataFrame returned in question 10.

    Returns:
        DataFrame: The pivot table.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    pivot_table = jobs_df.pivot_table(
        values='salary_in_aud', 
        index='country', 
        columns='experience_rating', 
        aggfunc='mean'
    )
    
    pivot_table.fillna(0, inplace=True)
    pivot_table = pivot_table.astype(int)
    pivot_table.sort_values(by=[(1), (2), (3), (4)], ascending=[False, False, False, False], inplace=True)
    df = pivot_table.copy()
    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    log("QUESTION 11", output_df=None, other=df)
    return df


######################################################
# NOTE: DO NOT MODIFY THE FUNCTION SIGNATURE BELOW ...
######################################################
def question_12(jobs_df):
    """Create a visualisation of data science jobs to help inform a decision
    about where to live, based (minimally) on salary and cost of living.

    See the assignment spec for more details.

    Args:
        jobs_df (DataFrame): The jobs DataFrame returned in question 10.
    """

    ######################################################
    # TODO: Your code goes here ...
    ######################################################
    studentid="zxxxxx"
    df=jobs_df.copy()
    five_countries = ['Australia', 'France', 'Singapore', 'Spain', 'Ireland']
    df = df[df['country'].isin(five_countries)]
    grouped = df.groupby('country')[['salary_in_aud', 'cost_of_living']].mean()
    job_counts = df['country'].value_counts()
    plot_data = grouped.join(job_counts.rename('job_count'))
    plt.scatter(plot_data['cost_of_living'], plot_data['salary_in_aud'], s=plot_data['job_count']*10,  alpha=0.5)
    plt.title('Average Salary and Cost of Living by Country')
    plt.xlabel('Cost of Living ')
    plt.ylabel('Average Salary in AUD')
    plt.grid(True)
    for i, r in enumerate(plot_data.index):
        plt.annotate(r, (plot_data['cost_of_living'].iloc[i], plot_data['salary_in_aud'].iloc[i]))

    ######################################################
    # NOTE: DO NOT MODIFY THE CODE BELOW ...
    ######################################################
    plt.savefig(f"{studentid}-Q12.png")


######################################################
# NOTE: DO NOT MODIFY THE MAIN FUNCTION BELOW ...
######################################################
if __name__ == "__main__":
    # data ingestion and cleaning
    df1 = question_1("ds_jobs.csv")
    df2 = question_2("cost_of_living.csv", 
                     "https://www.cse.unsw.edu.au/~cs9321/24T1/ass1/cost_of_living.html")
    df3 = question_3("exchange_rates.csv", 
                     "https://www.cse.unsw.edu.au/~cs9321/24T1/ass1/exchange_rates.html")
    df4 = question_4("country_codes.csv", 
                     "https://www.cse.unsw.edu.au/~cs9321/24T1/ass1/country_codes.html")

    # data exploration
    df5 = question_5(df1.copy(True))

    # data manipulation
    df6 = question_6(df1.copy(True))
    df7 = question_7(df6.copy(True), df4.copy(True))
    df8 = question_8(df7.copy(True), df3.copy(True))
    df9 = question_9(df2.copy(True))
    df10 = question_10(df8.copy(True), df9.copy(True))
    df11 = question_11(df10.copy(True))

    # data visualisation
    question_12(df10.copy(True))
