#!/usr/bin/env python3
"""
Author: Anna-Marie Seelen
Description: Takes a file with MassQL queries, searches for spectra in json file that contain the queries and writes
these spectra to individual files in mgf-style.
Usage: python3 massql_search_spectra_with_motif.py *path_to_file_with_motifs_queries*

    path_to_file_with_motifs_queries: a tab separated file with a selected motif, feature list and massql query on each
    line (output from make_pdf_with_smiles.py)
    pickle_file_with_gnps_all_positive_MS/MS_spectra: path to pickle file which is in:
    /mnt/LTR_userdata/hooft001/mass_spectral_embeddings/datasets/GNPS_15_12_21/ALL_GNPS_15_12_2021_positive_annotated.pickle
    path_to_store_spectrum_files: folder where all the mgf-formatted text files with spectra will be stored that contain
    a selected motif (determined by MassQL)
    path_to_store_match_files: folder where per Mass2Motif query a file will be stored containing the amount of massql
    matches and the smiles of each selected library spectrum
    path_to_store_mgf_file_and_name: the path and the file name that you want for the mgf-formatted MS/MS spectra from
    GNPS
    path_to_store_json_file_and_name: the path and the file name that you want for the json MS/MS spectra from
    GNPS
"""
#TODO: assumptions RAM and CPUs are always int. requested Ram is always in GB, the records started in 2024
#TODO: add option to run the code just for a certain month
#TODO: also have a way to extarct runs out of there that are below 10 seconds

# import statements
import pandas as pd
import calendar
import datetime

def make_dates_from_months(start_month: int, end_month:int, year:int):
    if start_month > end_month:
        raise ValueError("start_month must be less than end_month")

    date_of_start_month = datetime.datetime(year, start_month, 1)
    print(date_of_start_month)

    if end_month == datetime.datetime.now().month:
        #TODO: make a warning out of this
        print("end month is the current month, so will calculate amount of hours up until now")
        date_of_end_month = datetime.datetime.now()
    else:
        first, last = calendar.monthrange(year, end_month)
        date_of_end_month = datetime.datetime(year, end_month, last, 23, 59, 59)
    print(date_of_end_month)
    return (date_of_start_month, date_of_end_month)
def average_RAM_usage(df: pd.DataFrame, date_of_start_month, date_of_end_month) -> float:
    total_requested_RAM_time=0
    for index, row in df.iterrows():
        if row['SubmitTime'] in list(pd.date_range(date_of_start_month, date_of_end_month - datetime.timedelta(days=1), freq='d')):
            total_requested_RAM_time+=row['requested_RAM_GB']*row['RunTime_hours']
    return total_requested_RAM_time

def average_CPU_usage(df: pd.DataFrame, date_of_start_month, date_of_end_month) -> float:
    total_requested_CPU_time=0
    for index, row in df.iterrows():
        if row['SubmitTime'] in list(pd.date_range(date_of_start_month, date_of_end_month - datetime.timedelta(days=1), freq='d')):
            total_requested_CPU_time+=row['requested_CPUs']*row['RunTime_hours']
    return total_requested_CPU_time

def calculate_amount_of_hours_in_months(date_of_start_month, date_of_end_month) -> float:
    hours = (date_of_end_month - date_of_start_month).total_seconds() // 3600
    return hours


def main():
    """Main function of this module"""
    # check if the directory has only .record files
    # step 1: parse the file and put accession num, organism name and dna sequence in a nested dict
    df = pd.DataFrame(list_for_df)
    date_of_start_month, date_of_end_month=make_dates_from_months(3, 3, 2024)
    total_requested_RAM_time=average_RAM_usage(df,date_of_start_month,date_of_end_month)
    total_requested_CPU_time=average_CPU_usage(df, date_of_start_month, date_of_end_month)
    hours=calculate_amount_of_hours_in_months(date_of_start_month, date_of_end_month)
    average_requested_RAM=total_requested_RAM_time/hours
    average_requested_CPU=total_requested_CPU_time/hours
    print(f"average RAM usage {average_requested_RAM:.2f},average CPU usage {average_requested_CPU:.2f}")

if __name__ == "__main__":
    main()
