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

# import statements
from sys import argv
import os.path
import re
import datetime
import pandas as pd
import numpy as np

#TODO: assumptions RAM and CPUs are always int. requested Ram is always in GB, the records started in 2024
#TODO: still extract this from the scripts: % User (Computation): 97.88% etc.
#% System (I/O)      :  2.12%
#Mem reserved        : 500G
#Max Mem used        : 271.09G (cn136)
#Max Disk Write      : 81.92K (cn136)
#Max Disk Read       : 26.83M (cn136)
#TODO: also extract the date so you can do these stuff by month
#TODO: add option to run the code just for a certain month
#TODO: sometimes when you have a failed thing you don't have effieciency
# TODO: also have a way to extarct runs out of there that are below 10 seconds
# functions
def parse_input_file(slurm_record_filepath: str) -> tuple:
    with open(slurm_record_filepath, "r") as lines_slurm_file:
        filetext = lines_slurm_file.read()
        JobID = re.search(r'JobId=(.*) ', filetext).group(1)
        JobName = re.search(r'JobName=(.*)', filetext).group(1)
        requested_CPUs = int(re.search(r'NumCPUs=(\d*)', filetext).group(1))
        UserID=re.search(r'UserId=([a-z]+)\(.*\)', filetext).group(1)
        WorkDir=re.search(r'WorkDir=(.*)\n', filetext).group(1)
        # Efficiency sometimes is not given if the code has not run long enough
        if re.search(r'Used CPU time       : (.*) \(efficiency:[\s]*(([0-9]*[.])?[0-9]+)%\)', filetext) is not None:
            Efficiency = float(re.search(r'Used CPU time       : (.*) \(efficiency:[\s]*(([0-9]*[.])?[0-9]+)%\)', filetext).group(2))
        else:
            Efficiency = None

        CPU_Computation = float(
            re.search(r'User \(Computation\):[\s]+(([0-9]*[.])?[0-9]+)%', filetext).group(1))
        CPU_IO = float(re.search(r'System \(I\/O\)      :[\s]+(([0-9]*[.])?[0-9]+)%', filetext).group(1))
    return (JobID, JobName, requested_CPUs,UserID, WorkDir, Efficiency, CPU_Computation, CPU_IO)

def parse_input_file_for_recalculation(slurm_record_filepath: str) -> list:
    with open(slurm_record_filepath, "r") as lines_slurm_file:
        filetext = lines_slurm_file.read()

        requested_RAM = int(re.search(r'mem=(\d*)([A-Z]+)', filetext).group(1))
        RAM_unit = re.search(r'mem=(\d*)([A-Z]+)', filetext).group(2)

        Max_RAM_used = float(re.search(r'Max Mem used        : (([0-9]*[.])?[0-9]+)([A-Z]+)', filetext).group(1))
        Max_RAM_used_unit = re.search(r'Max Mem used        : (([0-9]*[.])?[0-9]+)([A-Z]+)', filetext).group(3)

        Max_Disk_Write = float(re.search(r'Max Disk Write      : (([0-9]*[.])?[0-9]+)([A-Z]*)', filetext).group(1))
        # If Max_Disk_Write is zero then it has no unit
        if re.search(r'Max Disk Write      : (([0-9]*[.])?[0-9]+)([A-Z]+)', filetext) is not None:
            Max_Disk_write_unit = re.search(r'Max Disk Write      : (([0-9]*[.])?[0-9]+)([A-Z]+)', filetext).group(
                3)
        else:
            Max_Disk_write_unit = None

        Max_Disk_Read = float(re.search(r'Max Disk Read       : (([0-9]*[.])?[0-9]+)([A-Z]*)', filetext).group(1))
        # If Max_Disk_Read is zero then it has no unit
        if re.search(r'Max Disk Read       : (([0-9]*[.])?[0-9]+)([A-Z]+)', filetext) is not None:
            Max_Disk_read_unit = re.search(r'Max Disk Read       : (([0-9]*[.])?[0-9]+)([A-Z]+)', filetext).group(3)
        else:
            Max_Disk_read_unit = None
    return [(requested_RAM,RAM_unit),(Max_RAM_used,Max_RAM_used_unit),(Max_Disk_Write,Max_Disk_write_unit),(Max_Disk_Read,Max_Disk_read_unit)]

def parse_input_file_time_parameters(slurm_record_filepath: str) -> tuple:
    with open(slurm_record_filepath, "r") as lines_slurm_file:
        filetext = lines_slurm_file.read()
        SubmitTime = re.search(r'SubmitTime=(.*)T(.*) ', filetext).group(1)
        RunTime = re.search(r'RunTime=((\d*)-?(\d*):(\d*):(\d*))', filetext).group(1)
    return(SubmitTime,RunTime)

def recalculate_to_GB(num,unit):
    if unit == 'M':
        num_in_GB=recalculate_MB_to_GB(num)
    elif unit == 'K':
        num_in_GB=recalculate_MB_to_GB(num)
    # if unit is None or if unit is G nothing needs to happen
    else:
        num_in_GB=num
    return num_in_GB

def recalculate_MB_to_GB(MB):
    GB = MB / 1024
    return GB
def recalculate_KB_to_GB(KB):
    GB = KB / (1024*1024)
    return GB

def recalculate_time_to_hours(RunTime):
        run_time_in_hours=0
        if re.search(r'(\d+)-(\d*):(\d*):(\d*)', RunTime) is None:
            run_time_hours = int(re.search(r'(\d*):(\d*):(\d*)', RunTime).group(1))
            run_time_in_hours += run_time_hours
            run_time_minutes = int(re.search(r'(\d*):(\d*):(\d*)', RunTime).group(2))
            run_time_in_hours += (run_time_minutes / 60)
            run_time_seconds = int(re.search(r'(\d*):(\d*):(\d*)', RunTime).group(3))
            run_time_in_hours += (run_time_seconds / 3600)
        else:
            run_time_days=int(re.search(r'(\d+)-(\d*):(\d*):(\d*)', RunTime).group(1))
            run_time_in_hours+=(24*run_time_days)
            run_time_hours = int(re.search(r'(\d*)-(\d*):(\d*):(\d*)', RunTime).group(2))
            run_time_in_hours += run_time_hours
            run_time_minutes = int(re.search(r'(\d*)-(\d*):(\d*):(\d*)', RunTime).group(3))
            run_time_in_hours += (run_time_minutes / 60)
            run_time_seconds = int(re.search(r'(\d*)-(\d*):(\d*):(\d*)', RunTime).group(4))
            run_time_in_hours += (run_time_seconds / 3600)
        return run_time_in_hours

def main():
    """Main function of this module"""
    # check if the directory has only .record files
    # step 1: parse the file and put accession num, organism name and dna sequence in a nested dict
    path_to_record_dir=os.path.abspath(argv[1])
    dir_with_slurm=list(os.listdir(path_to_record_dir))
    print(dir_with_slurm)
    total_requested_RAM_time=0
    total_requested_CPU_time=0
    #dict_with_slurm_records = {}
    #for record in os.listdir():
    slurm_record_filepath=argv[1]
    list_for_df=[]
    for slurm_record in dir_with_slurm:
        dict_param_of_run={}
        print(slurm_record)

        slurm_record_filepath=os.path.join(path_to_record_dir,slurm_record)
        #parse input that doesn't need recalculation
        JobID, JobName, requested_CPUs,UserID, WorkDir, Efficiency, CPU_Computation, CPU_IO = parse_input_file(slurm_record_filepath)
        dict_param_of_run = {}
        #parse input that needs to be recalculated to GB
        list_of_param_and_unit=parse_input_file_for_recalculation(slurm_record_filepath)
        #parse input that needs to be recalculated to time
        SubmitTime,RunTime=parse_input_file_time_parameters(slurm_record_filepath)

        #recalculations GB
        for (parameter,unit) in list_of_param_and_unit:
            parameter_in_GB=recalculate_to_GB(parameter, unit)
            list_param_of_run.append(parameter_in_GB)

        #recalculations time
        RunTime = recalculate_time_to_hours(RunTime)
        SubmitTime = datetime.datetime.strptime(SubmitTime, '%Y-%m-%d')
        list_param_of_run.append(RunTime)
        list_param_of_run.append(SubmitTime)
        #add it to the dict
        print(list_param_of_run)
        for i in list_param_of_run:
            print(i)
        # add it to the list of dict
        list_for_df.append(dict_param_of_run)
        #recalculate run to hours
        #run_time_in_hours=recalculate_time_to_hours(RunTime)
        #calculate the times
        #total_requested_RAM_time+=requested_RAM*run_time_in_hours
        #total_requested_CPU_time+=requested_CPUs*run_time_in_hours
    df = pd.DataFrame(list_for_df)
    print(df)
    # now = datetime.datetime.now()
    # start_of_year=datetime.datetime(2024, 1, 1)
    # hours=(now-start_of_year).total_seconds()//3600
    # average_requested_RAM=total_requested_RAM_time/hours
    # average_requested_CPU=total_requested_CPU_time/hours
    # print(f"average RAM usage {average_requested_RAM:.2f},average CPU usage {average_requested_CPU:.2f}")

if __name__ == "__main__":
    main()
