#!/bin/sh

# Expects 4 parameters
#   1. The host IP/hostname where the master block lists are stored
#   2. The location on the host where the block lists are stored
#   3. The number of datanodes per host
#   4. Which datanode host index to copy the files from, starting from zero
#   5. The base target destination to copy the files to
#
# Assumes passwordless ssh is configured to allow each host to access the others.
set -e


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if (( $# != 5 )); then
    echo "Illegal number of parameters"
    exit 1
fi

# The first block list file to copy is driven by which DN number this is
# and the number of DNs per host. The first DN is index zero, second 1, etc.
# With 5 DNs per host the first will copy blockList_1 to blockList 5
# The next blockList_6 to 10 etc.
#   dn_index * DNs_per_host + 1 -> dn_index * DNs_per_host + DNs_per_host
let "first = $4 * $3 + 1"
let "last = $4 * $3+ $3"
let "iteration = 1"

for ((i=$first; i<=$last; i++))
do
  scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $1:$2/blockList_$i $5/$iteration/blockList
  let iteration++
done
