#!/usr/bin/env bash
#SBATCH --job-name=DEDUP
#SBATCH --mail-user=lingsn@umich.edu
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --output=DEDUP_%u_%A_%a.out
#SBATCH --array=0-0
#SBATCH --account=cweidman99
#SBATCH --time=1:30:00
#SBATCH --mem=24000m
#SBATCH --cpus-per-task=8

module purge
eval "$(conda shell.bash hook)"
conda activate ~/miniconda3/envs/RNA-SEQ

declare -a tasks=(
"python parallel_dedup.py --bam star_align_2pass/H1299-NQO1YFP-Unaltered-normABX-DMSO-Rep1-S1/H1299-NQO1YFP-Unaltered-normABX-DMSO-Rep1-S1_cutadapt_fastp_filtered_star.bam -C 8"
)
eval ${tasks[$SLURM_ARRAY_TASK_ID]}
