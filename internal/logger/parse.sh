while [[ 1 ]]; do

if [ ! -f cpu ];
then
   exit
fi

temp=`cat cpu | tail -1`
idle=`echo $temp | awk '{print $15}'`
free=`echo $temp | awk '{print $4}'`
free_m=`echo "$free / 1024 "|bc`
mem=`expr 65536 - $free_m`
cpu_util=`expr 100 - $idle`

if [ ! -f network ] ;
then 
    exit
fi

temp=`cat network | tail -1`
in=`echo $temp | awk '{print $1}'`
out=`echo $temp | awk '{print $2}'`
total_network=`echo "$in + $out" | bc`
network_util=`echo "scale=2; $total_network * 100 / 1048576"| bc`

if [ ! -f disk ];
then
     exit
fi

temp=`cat disk | tail -5 |  grep sdb`
sdb=`echo $temp | awk '{print $14}'`

temp=`cat disk | tail -5 |  grep sdc`
sdc=`echo $temp | awk '{print $14}'`

temp=`cat disk | tail -5 |  grep sdd`
sdd=`echo $temp | awk '{print $14}'`
sum_disk=`echo "$sdb + $sdc + $sdd"| bc`
disk_util=`echo "scale=2; $sum_disk / 3" | bc`
echo $cpu_util $network_util $disk_util $mem> summary

sleep $1 
done
