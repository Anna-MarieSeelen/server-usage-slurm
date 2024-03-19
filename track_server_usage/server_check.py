#!/usr/bin/env python3
"""
Author: Anna-Marie Seelen
Studentnumber:1008970
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
import subprocess
import os.path
import re
import string
import datetime

#TODO: assumptions RAM and CPUs are always int. Ram is always in GB, the records started in 2024

# functions
def parse_input(slurm_record_filepath: str) -> dict:
    with open(slurm_record_filepath, "r") as lines_slurm_file:
        filetext = lines_slurm_file.read()
        JobID = re.search(r'JobId=(.*) ', filetext).group(1)
        RAM = int(re.search(r'mem=(\d*)', filetext).group(1))
        CPUs = int(re.search(r'NumCPUs=(\d*)', filetext).group(1))
        run_time=0
        if re.search(r'RunTime=(\d+)-(\d*):(\d*):(\d*)', filetext) is None:
            run_time_hours = int(re.search(r'RunTime=(\d*):(\d*):(\d*)', filetext).group(1))
            run_time += run_time_hours
            run_time_minutes = int(re.search(r'RunTime=(\d*):(\d*):(\d*)', filetext).group(2))
            run_time += (run_time_minutes / 60)
            run_time_seconds = int(re.search(r'RunTime=(\d*):(\d*):(\d*)', filetext).group(3))
            run_time += (run_time_seconds / 3600)
        else:
            run_time_days=int(re.search(r'RunTime=(\d+)-(\d*):(\d*):(\d*)', filetext).group(1))
            run_time+=(24*run_time_days)
            run_time_hours = int(re.search(r'RunTime=(\d*)-(\d*):(\d*):(\d*)', filetext).group(2))
            run_time += run_time_hours
            run_time_minutes = int(re.search(r'RunTime=(\d*)-(\d*):(\d*):(\d*)', filetext).group(3))
            run_time += (run_time_minutes / 60)
            run_time_seconds = int(re.search(r'RunTime=(\d*)-(\d*):(\d*):(\d*)', filetext).group(4))
            run_time += (run_time_seconds / 3600)
    return (JobID,RAM,CPUs,run_time)



def main():
    """Main function of this module"""
    # check if the directory has only .record files
    # step 1: parse the file and put accession num, organism name and dna sequence in a nested dict
    path_to_record_dir=os.path.abspath(argv[1])
    dir_with_slurm=list(os.listdir(path_to_record_dir))
    print(dir_with_slurm)
    RAM_time=0
    CPU_time=0
    #dict_with_slurm_records = {}
    #for record in os.listdir():
    slurm_record_filepath=argv[1]
    for slurm_record in dir_with_slurm:
        slurm_record_filepath=os.path.join(path_to_record_dir,slurm_record)
        #parse input
        #recalculate run to hours
        JobID,RAM,CPUs,run_time=parse_input(slurm_record_filepath)
        #calculate the times
        RAM_time+=RAM*run_time
        CPU_time+=CPUs*run_time
    now = datetime.datetime.now()
    start_of_year=datetime.datetime(2024, 1, 1)
    hours=(now-start_of_year).total_seconds()//3600
    average_RAM=int(RAM_time/hours)
    average_CPU=int(CPU_time/hours)
    print(f"average RAM usage {average_RAM},average CPU usage {average_CPU}")

if __name__ == "__main__":
    main()
