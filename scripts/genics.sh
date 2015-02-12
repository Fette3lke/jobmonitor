#!/bin/bash -l
#echo $HOSTNAME >> /u/loser/dum
ID_DIR="/u/sciteam/oser/scratch/P200/idlists"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BINDIR="/u/sciteam/oser/scratch/REFINE-P200/bin"
export LD_LIBRARY_PATH="$GSL_LIBDIR"
cd $DIR
cd $OLDPWD
module swap PrgEnv-cray PrgEnv-gnu
env > ${1}.env

#EXPORTS
source exports.sh

if [ ! $JM_LEVELMAX ]; then JM_LEVELMAX="12"; fi
if [ ! $JM_NAME_ADD ]; then JM_NAME_ADD=""; fi
if [ ! $JM_OMP_THREADS ]; then JM_OMP_THREADS="16"; fi
if [ ! $JM_OUTFILE ]; then JM_OUTFILE="tmp/${JM_PREFIX}${1}.out"; fi
if [ ! $JM_PREFIX ]; then JM_PREFIX="P"; fi
if [ ! $JM_BARYONS ]; then JM_BARYONS="yes"; fi
PREFIX=$JM_PREFIX
OF=$JM_OUTFILE

echo "refining $1" > $OF
echo "host $HOSTNAME" >> $OF
echo "bin-dir " $BINDIR >> $OF

#sed -n ${2}p $TMPDIR/machines > tmp/machinefile

if [ ! -s "tmp/positions_$1.dat" ]
then
    $BINDIR/pos2ascii $ID_DIR/idlist_positions_$1 > tmp/positions_$1.dat
fi


#exit
IC_FILE_NAME="$PWD/ic_${JM_PREFIX}${1}_${JM_LEVELMAX}${JM_NAME_ADD}.gdt"
echo $IC_FILE_NAME >> $OF
if [ ! -s "${IC_FILE_NAME}.0" ]
then
    PARAM_FILE="$PWD/tmp/music_param_${JM_PREFIX}${1}_lm${JM_LEVELMAX}.inp"
#    XCENTRE=$( perl -e "printf(\"%8.6f\", ((split(\" \",'$MASK_PARS'))[0]))")
#    YCENTRE=$( perl -e "printf(\"%8.6f\", ((split(\" \",'$MASK_PARS'))[1]))")
#    ZCENTRE=$( perl -e "printf(\"%8.6f\", ((split(\" \",'$MASK_PARS'))[2]))")
#    SIDE=$( perl -e "printf(\"%8.6f\", ((split(\" \",'$MASK_PARS'))[3]))")
    cp $BINDIR/ics-P200.conf.template $PARAM_FILE    
    perl -pi -e "s%LEVELMAX%${JM_LEVELMAX}%; \
                 s%POINTFILE%tmp/positions_${1}.dat%; \
                 s%BARYONS%${JM_BARYONS}%; \
                 s%OUTFILENAME%${IC_FILE_NAME}%;" $PARAM_FILE 

#    aprun -n $NUM_MPI_THREADS -N 16 -d 2 $BINDIR/comp_disp.x $PARAM_FILE &> tmp/comp_disp.out
    echo "running MUSIC" >> $OF 
    export OMP_NUM_THREADS=$JM_OMP_THREADS
#    mpiexec -ppn 1 $BINDIR/MUSIC $PARAM_FILE &> tmp/music.out
    $BINDIR/MUSIC $PARAM_FILE &> tmp/music.out

fi

if [ -s "${IC_FILE_NAME}.0" ]
then
    mkdir IC
    mv ${IC_FILE_NAME}* IC/
    $BINDIR/join_gadfiles -ct -i "IC/ic_${JM_PREFIX}${1}_${JM_LEVELMAX}${JM_NAME_ADD}.gdt" -o "IC/ic_${JM_PREFIX}${1}_${JM_LEVELMAX}${JM_NAME_ADD}_cmb.gdt" >> $OF
    mv input_powerspec*txt tmp/
    rm -f wnoise_????.bin
fi

if [ -s "IC/ic_${JM_PREFIX}${1}_${JM_LEVELMAX}${JM_NAME_ADD}_cmb.gdt" ]
then
    echo "SUCCESS" >> $OF
fi
