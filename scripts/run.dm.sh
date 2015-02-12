#!/bin/bash -l
#echo $HOSTNAME >> /u/loser/dum
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BINDIR="/u/sciteam/oser/scratch/REFINE-P200/bin"
export LD_LIBRARY_PATH="$GSL_LIBDIR"
cd $DIR
cd $OLDPWD
module swap PrgEnv-cray PrgEnv-gnu
env > ${1}.env

#EXPORTS
source exports.sh

if [ ! $JM_NAME_ADD ]; then JM_NAME_ADD=""; fi
if [ ! $JM_PREFIX ]; then JM_PREFIX="P"; fi
if [ ! $JM_OUTFILE ]; then JM_OUTFILE="${JM_PREFIX}${1}.out"; fi
PREFIX=$JM_PREFIX
NAME="${PREFIX}${1}${NAME_ADD}"
HALO="${PREFIX}${1}"
OF=$JM_OUTFILE
CPU_PER_NODE=16

echo "simulating $1" > $OF

if [ -s "stop" ]
then
    rm -f stop
fi

PARAMFILE="refine_dm_${NAME}.tex"
ICNAME="/u/sciteam/oser/scratch/REFINE-P200/${HALO}/IC/ic_${HALO}_12_4x_dm.gdt"

if [ ! -s $PARAMFILE ]
then
    cp $BINDIR/refine.dm.template $PARAMFILE
    perl -pi -e "s%ICFILE%${ICNAME}%; \
                 s%OUTDIR%${PWD}%; \
                 s%SIMNAME%${NAME}%; \
                 " $PARAMFILE 

    cp $BINDIR/P-Gadget3-DM ./
fi

if [ ! $JM_NUMNODES ]
then 
    JM_NUMNODES=`wc -l $JM_NODES | cut -f 1 -d " "`
fi

NUM_CPUS=$(($JM_NUMNODES * $CPU_PER_NODE))

if test -n "$(find ./restartfiles -maxdepth 1 -name 'restart_*.0' -print -quit)"
then
    #restart
    echo "aprun -n $NUM_CPUS -N $CPU_PER_NODE -j 1 -l $JM_NODES ./P-Gadget3-DM $PARAMFILE 1" >> $OF
    aprun -n $NUM_CPUS -N $CPU_PER_NODE -j 1 -l $JM_NODES ./P-Gadget3-DM $PARAMFILE 1 &> gadget.out
else
    #start
    echo "aprun -n $NUM_CPUS -N $CPU_PER_NODE -j 1 -l $JM_NODES ./P-Gadget3-DM $PARAMFILE" >> $OF
    aprun -n $NUM_CPUS -N $CPU_PER_NODE -j 1 -l $JM_NODES ./P-Gadget3-DM $PARAMFILE &> gadget.out
fi

if test -n "$(find ./ -maxdepth 1 -name 'snap*094' -print -quit)"
then
    echo "SUCCESS" >> $OF
fi
