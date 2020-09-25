#!/bin/bash
#SBATCH --job-name=load_graph       # Job name
#SBATCH --mail-type=END,FAIL        # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=powen@renci.org # Where to send mail
#SBATCH --ntasks=1                  # Run on a single CPU
#SBATCH --mem=512gb                 # Job memory request
#SBATCH --time=12:00:00             # Time limit hrs:min:sec
#SBATCH --output=load_graph%j.log   # Standard output and error log

hostname; date

source /home/powen/venv3.8/bin/activate
cd /home/powen/kgx
export PYTHONPATH=$PWD

echo $PYTHONPATH

echo $PWD

python --version

echo "Running KGX to load GTEx graph"

python examples/scripts/load_json_to_neo4j.py --uri http://robokopdev.renci.org:7490 --node_file /projects/stars/Data_services/GTEx_data/gtex_kgx_nodes.json --edge_file /projects/stars/Data_services/GTEx_data/gtex_kgx_edges.json --username neo4j --password ncatsgamma
#python examples/scripts/load_json_to_neo4j.py --uri http://robokopdev.renci.org:7490 --node_file /projects/stars/Data_services/GTEx_data/Human_GOA_nodes.json --edge_file /projects/stars/Data_services/GTEx_data/Human_GOA_edges.json --username neo4j --password ncatsgamma

date
