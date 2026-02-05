## Use RNA-STAR conda environment
from pathlib import Path
import traceback
import argparse
import textwrap

def main(input_dir, email, slurm_acct, walltime, mem, fa):
    '''
    PURPOSE:
    * Goes into folder containing all raw FASTQs and obtains all sample names.
    * Automatically appends new task containing sample name to SBATCH script,
      which then trims adapters for all raw FASTQs.
    ---
    EXAMPLE:
    1. Given the file:
        -> KEH-Rep1-7KO-HEK293T-Cyto-BS_S6_L001_R1_001.fastq.gz
       We obtain the sample name:
        -> KEH-Rep1-7KO-HEK293T-Cyto-BS_S6
    2. We append the following task to the SBATCH script:
       "python3 run_cutadapt_fastp.py --input raw_fastqs --output trimmed_reads \ 
       -C 2 -U 12 -S KEH-Rep1-7KO-HEK293T-Cyto-BS_S6"
    '''
    current_path = Path.cwd()
    output = Path("SBATCHSubArr-CUT_FASTP.sbatch")
    
    start_dir = current_path/"realignments"
    if not start_dir.exists():
        raise FileNotFoundError(
            "Realignments folder doesn't exist. Did you run realignGap.py?"
        )
    
    ## Create SBATCH file if it doesn't exist
    if not output.exists():
        with open(output, "w") as f:
            template_start = textwrap.dedent(f"""\
                                            #!/usr/bin/env bash
                                            #SBATCH --job-name=calculate_dr
                                            #SBATCH --mail-user={email}
                                            #SBATCH --mail-type=BEGIN,END,FAIL
                                            #SBATCH --output=calculate_dr_%u_%A_%a.out
                                            #SBATCH --array=0-{num_jobs}
                                            #SBATCH --account={slurm_acct}
                                            #SBATCH --time={walltime}
                                            #SBATCH --mem={mem}m
                                            #SBATCH --partition=standard
                                            #SBATCH --ntasks-per-node=1
                                            #SBATCH --nodes=1
                                            ################################################################################
                                            # Edit the strings under 'declare -a tasks=(' to match your experiments.
                                            #
                                            # The #SBATCH --array variable above creates an array [0,1,2,3]. Change it so that the length
                                            # is how many jobs you need (same as number of strings under $tasks).
                                            #
                                            # This script is submitted that many times, but only one line from $tasks is
                                            # evaluated each time.
                                            #
                                            # For more info on #SBATCH variables, see https://arc.umich.edu/greatlakes/slurm-user-guide/
                                            # and https://slurm.schedmd.com/sbatch.html
                                            #
                                            # This requires a conda environment with samtools and pysam (RNA-STAR)
                                            # 
                                            # To call this script:
                                            # sbatch run_calculate_dr.sbatch
                                            ################################################################################

                                            module purge
                                            eval "$(conda shell.bash hook)"
                                            conda activate ~/miniconda3/envs/RNA-STAR

                                            declare -a tasks=(
                                            """)

            f.write(template_start)

    try:
        for folder in start_dir.iterdir():
            ## Obtain all subfolder names in each folder within 'realignments'
            subfolders = [subf.stem for subf in folder.iterdir() if subf.is_dir()]

            ## Append new tasks to SBATCH
            with open(output, "a") as f:
                for subf in subfolders:
                    task = f"\npython3 run_calculate_dr.py --folder_name {folder} --subf_name {subf} --fasta {fa}"
                    f.write(task)
        
        ## Once all tasks have been added, finish up SBATCH template
        with open(output, "a") as f:
            f.write("\n)\neval ${tasks[$SLURM_ARRAY_TASK_ID]}")
    except Exception as e:
        print("Failed to create SBATCH file: {e}")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Writes SBATCH script for adapter trimming.")
    parser.add_argument("--input_folder", help = "Name of folder (NOT DIRECTORY) containing all raw FASTQs", required = True)
    parser.add_argument("--email", default = "<uniqname>@umich.edu")
    parser.add_argument("--slurm_acct", default = "<account>")
    parser.add_argument("--walltime", required = True)
    parser.add_argument("--mem", help = "Memory for job", required = True)
    parser.add_argument("--fa", help = "Directory to FASTA file of reference genome", required = True)

    args = parser.parse_args()

    print("Writing SBATCH script...")
    main(args.email, args.slurm_acct, args.walltime, args.mem, args.fa)
    print("Process finished.")