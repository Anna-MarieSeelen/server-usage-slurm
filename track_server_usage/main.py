#!/usr/bin/env python3
"""
Author: Anna-Marie Seelen
Description: Takes a file with MassQL queries, searches for spectra in json file that contain the queries and writes
these spectra to individual files in mgf-style.
Usage: python3 massql_search_spectra_with_motif.py *path_to_file_with_motifs_queries*

"""
from sys import argv
import os.path
import datetime
import pandas as pd
import glob
from parse_input_to_df import recalculate_to_GB, parse_input_file_for_recalculation, parse_input_file, parse_input_file_time_parameters, recalculate_time_to_hours
from calculations import *

def main():
    """Main function of this module"""
    # step 1: parse the file and put accession num, organism name and dna sequence in a nested dict
    path_to_record_dir=os.path.abspath(argv[1])
    dir_with_slurm=list(os.listdir(path_to_record_dir))

    # check if the directory has only .record files
    files_with_record_ext = glob.glob(os.path.join(path_to_record_dir,'*.record'))
    if len(files_with_record_ext) != len(dir_with_slurm):
        raise ValueError('There are not only .record files in this directory. You are probably not in the right directory')

    list_for_df=[]
    for slurm_record in dir_with_slurm:
        slurm_record_filepath=os.path.join(path_to_record_dir,slurm_record)
        #parse input that doesn't need recalculation and add to dict
        JobID, JobName, requested_CPUs,UserID, WorkDir, Efficiency, CPU_Computation, CPU_IO = parse_input_file(slurm_record_filepath)
        dict_param_of_run = {'JobID': JobID,
                             'JobName': JobName,
                             'requested_CPUs': requested_CPUs,
                             'UserID': UserID,
                             'WorkDir': WorkDir,
                             'Efficiency': Efficiency,
                             'CPU_Computation': CPU_Computation,
                             'CPU_IO': CPU_IO}
        #parse input that needs to be recalculated to GB
        requested_RAM,RAM_unit,Max_RAM_used,Max_RAM_used_unit,Max_Disk_Write,Max_Disk_write_unit,Max_Disk_Read,Max_Disk_read_unit=parse_input_file_for_recalculation(slurm_record_filepath)
        #recalculate to GB and add to dict
        dict_param_of_run['requested_RAM_GB'] = recalculate_to_GB(requested_RAM, RAM_unit)
        dict_param_of_run['Max_RAM_used_GB'] = recalculate_to_GB(Max_RAM_used, Max_RAM_used_unit)
        dict_param_of_run['Max_Disk_Write_GB'] = recalculate_to_GB(Max_Disk_Write, Max_Disk_write_unit)
        dict_param_of_run['Max_Disk_Read_GB'] = recalculate_to_GB(Max_Disk_Read, Max_Disk_read_unit)

        #parse input that needs to be recalculated to time
        SubmitTime,RunTime=parse_input_file_time_parameters(slurm_record_filepath)
        #recalculations time
        dict_param_of_run['RunTime_hours'] = recalculate_time_to_hours(RunTime)
        dict_param_of_run['SubmitTime'] = datetime.datetime.strptime(SubmitTime, '%Y-%m-%d')

        list_for_df.append(dict_param_of_run)

    df = pd.DataFrame(list_for_df)
    print(df)
    date_of_start_month, date_of_end_month = make_dates_from_months(3, 3, 2024)
    total_requested_RAM_time = average_RAM_usage(df, date_of_start_month, date_of_end_month)
    total_requested_CPU_time = average_CPU_usage(df, date_of_start_month, date_of_end_month)
    hours = calculate_amount_of_hours_in_months(date_of_start_month, date_of_end_month)
    average_requested_RAM = total_requested_RAM_time / hours
    average_requested_CPU = total_requested_CPU_time / hours
    print(f"average RAM usage {average_requested_RAM:.2f},average CPU usage {average_requested_CPU:.2f}")
    #also do this for all users
    for i in users:
        mask = df['UserID'].isin(i)
        df_users = df[mask]

if __name__ == "__main__":
    main()
