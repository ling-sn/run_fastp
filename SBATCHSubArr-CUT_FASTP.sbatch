#!/usr/bin/env bash
#SBATCH --job-name=CUT_FASTP
#SBATCH --mail-user=lingsn@umich.edu
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --output=CUT_FASTP_%u_%A_%a.out
#SBATCH --array=0-0
#SBATCH --account=cweidman99
#SBATCH --time=1:00:00
#SBATCH --mem=10000m
#SBATCH --cpus-per-task=4
################################################################################
# Edit the strings under 'declare -a tasks=(' to match your experiments.
#
# Recommend 45 minutes for every sample.
#
# This requires a conda environment for genome alignment, edit to your named version in the activate command
# 
# To call this script:
# sbatch SBATCH-arrXXX.sbatch
################################################################################

module purge
eval "$(conda shell.bash hook)"
conda activate ~/miniconda3/envs/RNA-SEQ

declare -a tasks=(
"python run_cutadapt_fastp.py --input Step1_Reads --output test_trimmed_reads -C 4 -U 12 -S H1299-NQO1YFP-Unaltered-normABX-DMSO-Rep1-S1"
)
eval ${tasks[$SLURM_ARRAY_TASK_ID]}

