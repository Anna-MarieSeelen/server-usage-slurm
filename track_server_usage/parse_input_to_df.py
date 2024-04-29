#!/usr/bin/env python3
"""Author: Anna-Marie Seelen
Description: Takes a file with MassQL queries, searches for spectra in json file that contain the queries and writes
these spectra to individual files in mgf-style.
Usage: python3 massql_search_spectra_with_motif.py *path_to_file_with_motifs_queries*
"""

# import statements
import datetime
import glob
import os.path
import re
from sys import argv
import pandas as pd


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
    return (requested_RAM,RAM_unit,Max_RAM_used,Max_RAM_used_unit,Max_Disk_Write,Max_Disk_write_unit,Max_Disk_Read,Max_Disk_read_unit)

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

if __name__ == "__main__":
    main()
