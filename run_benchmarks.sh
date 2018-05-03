#!/bin/bash

cd "$(dirname ${BASH_SOURCE[0]})"

###############################
### Environment Information ###
###############################
sudo apt-get update -y
sudo apt-get install hwinfo numactl -y
timestamp=$(date -u +%s)
run_uuid=$(uuidgen)
nodeid=$(cat /var/emulab/boot/nodeid)
nodeuuid=$(cat /var/emulab/boot/nodeuuid)
gcc_ver=$(gcc --version | grep gcc | awk '{print $4}')

# HW info, no PCI bus on ARM means lshw doesn't have as much information
nthreads=$(nproc --all)
total_mem=$(sudo lshw -short -c memory | grep System | awk '{print $3}')
arch=$(uname -m)
kernel_release=$(uname -r)
# Because ARM has to do cpuinfo so differently, hardcode for non x86_64...
nsockets=1
if [ ${arch} == 'x86_64' ]; then
    nsockets=$(cat /proc/cpuinfo | grep "physical id" | sort -n | uniq | wc -l)
    mem_clock_speed=$(sudo dmidecode --type 17  | grep "Configured Clock Speed" | head -n 1 | awk '{print $4}')
    mem_clock_speed=${mem_clock_speed}MHz
elif [ ${arch} == 'aarch64' ]; then
    mem_clock_speed="Unknown(ARM)"
else
    # Temp placeholder for unknown architecture
    mem_clock_speed="Unknown(Unknown_Arch)"
fi

# Hash
version_hash=$(git rev-parse HEAD)

# Write to file
echo "run_uuid,timestamp,nodeid,nodeuuid,arch,gcc_ver,version_hash,total_mem,mem_clock_speed,nthreads,nsockets,kernel_release" > ~/env_out.csv
echo "$run_uuid,$timestamp,$nodeid,$nodeuuid,$arch,$gcc_ver,$version_hash,$total_mem,$mem_clock_speed,$nthreads,$nsockets,$kernel_release" >> ~/env_out.csv

###########################
### Network Information ###
###########################
# Grab the physical interface serving the VLAN
vlan_to_link=$(sudo ip link | grep vlan | awk '{print $2}' | tr '@' ' ' | tr -d ':')

# Parse VLAN info
vlan_name=$(echo $vlan_to_link | awk '{print $1}')
vlan_ip=$(sudo ifconfig $vlan_name | grep 'inet addr' | awk '{print $2}' | cut -d \: -f 2)
vlan_hwaddr=$(sudo ifconfig $vlan_name | grep HWaddr | awk '{print $5}')
vlan_driver=$(sudo ethtool -i $vlan_name | grep driver | awk '{print substr($0, index($0, $2))}')
vlan_driver_ver=$(sudo ethtool -i $vlan_name | grep version | grep -v -E 'firmware|rom' | awk '{print substr($0, index($0, $2))}')

# Parse interface info
if_name=$(echo $vlan_to_link | awk '{print $2}')
if_hwaddr=$(sudo ifconfig $if_name | grep HWaddr | awk '{print $5}')
# Utah machines like to put the vendor information on the parent, so here's a workaround
if [[ $(python -c "res = True if 'ms' in '$(cat /var/emulab/boot/nodeid)' else False; print res") = True ]]; then
    if_hwinfo=$(sudo lshw -class network -businfo | grep ${if_name::-2} | grep -v $if_name | awk '{print substr($0, index($0, $4))}')
else
    if_hwinfo=$(sudo lshw -class network -businfo | grep $if_name | awk '{print substr($0, index($0, $4))}')
fi
if_speed=$(sudo ethtool $if_name | grep Speed | awk '{print $2}')
if_duplex=$(sudo ethtool $if_name | grep Duplex | awk '{print $2}')
if_port_type=$(sudo ethtool $if_name | grep Port | awk '{print substr($0, index($0, $2))}')
if_driver=$(sudo ethtool -i $if_name | grep driver | awk '{print substr($0, index($0, $2))}')
if_driver_ver=$(sudo ethtool -i $if_name | grep version | grep -v -E 'firmware|rom' | awk '{print substr($0, index($0, $2))}')
if_bus_location=$(sudo ethtool -i $if_name | grep bus | awk '{print $2}')

echo "run_uuid,timestamp,nodeid,nodeuuid,vlan_name,vlan_ip,vlan_hwaddr,vlan_driver,vlan_driver_ver,if_name,if_hwaddr,if_hwinfo,if_speed,if_duplex,if_port_type,if_driver,if_driver_ver,if_bus_location" > ~/net_info.csv
echo "$run_uuid,$timestamp,$nodeid,$nodeuuid,$vlan_name,$vlan_ip,$vlan_hwaddr,$vlan_driver,$vlan_driver_ver,$if_name,$if_hwaddr,$if_hwinfo,$if_speed,$if_duplex,$if_port_type,$if_driver,$if_driver_ver,$if_bus_location" >> ~/net_info.csv


#######################
### Network Latency ###
#######################

# First, define destination host and get the exposed dest_nodeid from server
net_server=192.168.1.100
dest_nodeid=$(curl $net_server:8000/nodeid)

# Run ping before everything else.
# Ping can potentially affect iperf3 results, so to be safe we run ping
# at such a point that it can never run at the same time as iperf3.
# Gather info and set up vars first
ping_version=$(ping -V | awk '{print $3}')
ping_count=10000
ping_size=56

echo "run_uuid,timestamp,nodeid,nodeuuid,ping_version,ping_count,ping_source_ip,ping_dest_ip,ping_dest_nodeid,ping_size" > ~/ping_info.csv
echo "$run_uuid,$timestamp,$nodeid,$nodeuuid,$ping_version,$ping_count,$vlan_ip,$net_server,$dest_nodeid,$ping_size" >> ~/ping_info.csv

# Run ping as a flood, and in quiet mode.  Must be sudo.
sudo ping -s $ping_size -f -q -c $ping_count $net_server > ~/temp_ping.out

pkts_sent=$(grep packets ~/temp_ping.out | awk '{print $1}')
pkts_received=$(grep packets ~/temp_ping.out | awk '{print $4}')
pkt_loss=$(grep packets ~/temp_ping.out | awk '{print $6}')
ping_time=$(grep packets ~/temp_ping.out | awk '{print $10}')

ping_stats=$(grep rtt ~/temp_ping.out | awk '{print $4}' | tr '/' ' ')
ping_min=$(echo $ping_stats | awk '{print $1}')
ping_avg=$(echo $ping_stats | awk '{print $2}')
ping_max=$(echo $ping_stats | awk '{print $3}')
ping_mdev=$(echo $ping_stats | awk '{print $4}')

ping_stats=$(grep rtt ~/temp_ping.out | awk '{print $7}' | tr '/' ' ')
ping_ipg=$(echo $ping_stats | awk '{print $1}')
ping_ewma=$(echo $ping_stats | awk '{print $2}')

ping_units=ms

echo "run_uuid,timestamp,nodeid,nodeuuid,runtime,packets_sent,packets_received,packet_loss,max,min,mean,stdev,ipg,ewma,units" > ~/ping_results.csv
echo "$run_uuid,$timestamp,$nodeid,$nodeuuid,$ping_time,$pkts_sent,$pkts_received,$pkt_loss,$ping_max,$ping_min,$ping_avg,$ping_mdev,$ping_ipg,$ping_ewma,$ping_units" >> ~/ping_results.csv

##############
### STREAM ###
##############

# DVFS init
dvfs="yes"

# Set up make vars
stream_ntimes=500
stream_array_size=10000000
stream_offset=0
stream_type=double
stream_optimization=O2
cd ./STREAM

# make from source and run
make clean
make NTIMES=$stream_ntimes STREAM_ARRAY_SIZE=$stream_array_size OFFSET=$stream_offset STREAM_TYPE=$stream_type OPT=$stream_optimization
echo "run_uuid,timestamp,nodeid,nodeuuid,stream_ntimes,stream_array_size,stream_offset,stream_type,stream_optimization" > ~/stream_info.csv
echo "$run_uuid,$timestamp,$nodeid,$nodeuuid,$stream_ntimes,$stream_array_size,$stream_offset,$stream_type,$stream_optimization" >> ~/stream_info.csv

for (( n=0; n<=$((nsockets-1)); n++ ))
do
    numactl -N $n ./streamc
    mv stream_out.csv ~/stream_out_socket${n}_dvfs.csv
    # Write to file
    sed -i '1s/$/,run_uuid,timestamp,nodeid,nodeuuid,socket_num,dvfs/' ~/stream_out_socket${n}_dvfs.csv
    sed -i "2s/$/,$run_uuid,$timestamp,$nodeid,$nodeuuid,$n,$dvfs/" ~/stream_out_socket${n}_dvfs.csv
done

################
### membench ###
################
# Set up make vars
membench_samples=5
membench_times=5
# LL is required due to int overflow issues
# Workaround to ensure that the size of the array is not only 32-byte aligned, but also evenly divisible by nthreads
membench_size=$(python -c "multiple=$nthreads * 32; list = [n for n in range(1024**3, 1024**3 + multiple) if n % multiple == 0]; print str(list[0]) + 'LL'")
membench_optimization=O3
cd ../membench

# make from source and run
make clean
make SAMPLES=$membench_samples TIMES=$membench_times SIZE=$membench_size OPT=$membench_optimization
echo "run_uuid,timestamp,nodeid,nodeuuid,membench_samples,membench_times,membench_size,membench_optimization" > ~/membench_info.csv
echo "$run_uuid,$timestamp,$nodeid,$nodeuuid,$membench_samples,$membench_times,$membench_size,$membench_optimization" >> ~/membench_info.csv
for (( n=0; n<=$((nsockets-1)); n++ ))
do
    numactl -N $n ./memory_profiler
    mv memory_profiler_out.csv ~/membench_out_socket${n}_dvfs.csv
    # Write to file
    sed -i '1s/$/,run_uuid,timestamp,nodeid,nodeuuid,socket_num,dvfs/' ~/membench_out_socket${n}_dvfs.csv
    sed -i "2s/$/,$run_uuid,$timestamp,$nodeid,$nodeuuid,$n,$dvfs/" ~/membench_out_socket${n}_dvfs.csv
done

############
### DVFS ###
############
if [ ${arch} == 'x86_64' ]; then
    # Turn DVFS stuff off, re-run memory experiments
    dvfs="no"
    sudo apt-get install msr-tools cpufrequtils -y
    sudo modprobe msr
    oldgovernor=$(sudo cpufreq-info -p | awk '{print $3}')
    for (( n=0; n<=$((nthreads-1)); n++ ))
    do
        sudo wrmsr -p$n 0x1a0 0x4000850089
        sudo cpufreq-set -c $n -g performance
    done
    
    
    # STREAM
    cd ../STREAM
    for (( n=0; n<=$((nsockets-1)); n++ ))
    do
        numactl -N $n ./streamc
        mv stream_out.csv ~/stream_out_socket${n}_nodvfs.csv
        # Write to file
        sed -i '1s/$/,run_uuid,timestamp,nodeid,nodeuuid,socket_num,dvfs/' ~/stream_out_socket${n}_nodvfs.csv
        sed -i "2s/$/,$run_uuid,$timestamp,$nodeid,$nodeuuid,$n,$dvfs/" ~/stream_out_socket${n}_nodvfs.csv
    done
    
    # membench
    cd ../membench
    for (( n=0; n<=$((nsockets-1)); n++ ))
    do
        numactl -N $n ./memory_profiler
        mv memory_profiler_out.csv ~/membench_out_socket${n}_nodvfs.csv
        # Write to file
        sed -i '1s/$/,run_uuid,timestamp,nodeid,nodeuuid,socket_num,dvfs/' ~/membench_out_socket${n}_nodvfs.csv
        sed -i "2s/$/,$run_uuid,$timestamp,$nodeid,$nodeuuid,$n,$dvfs/" ~/membench_out_socket${n}_nodvfs.csv
    done
    
    # Change everything back to normal
    for (( n=0; n<=$((nthreads-1)); n++ ))
    do
        sudo wrmsr -p$n 0x1a0 0x850089
        sudo cpufreq-set -c $n -g $oldgovernor
    done
fi

###########
### FIO ###
###########
cd ~
sudo apt-get install fio -y
fio_version=$(fio -v)

# Huge hardcoded FIO header, this is the worst...
fioheader="terse_version,fio_version,jobname,groupid,error,READ_kb,READ_bandwidth,READ_IOPS,READ_runtime,READ_Slat_min,READ_Slat_max,READ_Slat_mean,READ_Slat_dev,READ_Clat_max,READ_Clat_min,READ_Clat_mean,READ_Clat_dev,READ_clat_pct01,READ_clat_pct02,READ_clat_pct03,READ_clat_pct04,READ_clat_pct05,READ_clat_pct06,READ_clat_pct07,READ_clat_pct08,READ_clat_pct09,READ_clat_pct10,READ_clat_pct11,READ_clat_pct12,READ_clat_pct13,READ_clat_pct14,READ_clat_pct15,READ_clat_pct16,READ_clat_pct17,READ_clat_pct18,READ_clat_pct19,READ_clat_pct20,READ_tlat_min,READ_lat_max,READ_lat_mean,READ_lat_dev,READ_bw_min,READ_bw_max,READ_bw_agg_pct,READ_bw_mean,READ_bw_dev,WRITE_kb,WRITE_bandwidth,WRITE_IOPS,WRITE_runtime,WRITE_Slat_min,WRITE_Slat_max,WRITE_Slat_mean,WRITE_Slat_dev,WRITE_Clat_max,WRITE_Clat_min,WRITE_Clat_mean,WRITE_Clat_dev,WRITE_clat_pct01,WRITE_clat_pct02,WRITE_clat_pct03,WRITE_clat_pct04,WRITE_clat_pct05,WRITE_clat_pct06,WRITE_clat_pct07,WRITE_clat_pct08,WRITE_clat_pct09,WRITE_clat_pct10,WRITE_clat_pct11,WRITE_clat_pct12,WRITE_clat_pct13,WRITE_clat_pct14,WRITE_clat_pct15,WRITE_clat_pct16,WRITE_clat_pct17,WRITE_clat_pct18,WRITE_clat_pct19,WRITE_clat_pct20,WRITE_tlat_min,WRITE_lat_max,WRITE_lat_mean,WRITE_lat_dev,WRITE_bw_min,WRITE_bw_max,WRITE_bw_agg_pct,WRITE_bw_mean,WRITE_bw_dev,CPU_user,CPU_sys,CPU_csw,CPU_mjf,PU_minf,iodepth_1,iodepth_2,iodepth_4,iodepth_8,iodepth_16,iodepth_32,iodepth_64,lat_2us,lat_4us,lat_10us,lat_20us,lat_50us,lat_100us,lat_250us,lat_500us,lat_750us,lat_1000us,lat_2ms,lat_4ms,lat_10ms,lat_20ms,lat_50ms,lat_100ms,lat_250ms,lat_500ms,lat_750ms,lat_1000ms,lat_2000ms,lat_over_2000ms,disk_name,disk_read_iops,disk_write_iops,disk_read_merges,disk_write_merges,disk_read_ticks,write_ticks,disk_queue_time,disk_utilization,device,iod\n"

# iodepth set here, but we'll run tests both with this setting and iodepth=1
iodepth=4096
direct=1
numjobs=1
ioengine="libaio"
blocksize="4k"
size="10G"
# timeout is 12 minutes
timeout=720 

# This segment generates a list of block device targets for use in fio
testdevs=()
# Get the base raw block device names (sda, sdb, nvme0n1, etc...)
rawnames=($(sudo lsblk -d -io NAME | grep -v NAME | awk '{print $1}'))
for name in "${rawnames[@]}"
do
    # Check if base raw block device has partitions
    nparts=$(sudo fdisk -l /dev/$name | grep -v Disk | grep -c $name)
    if [ ${nparts} != 0 ]; then
        # If it does, check whether any are labeled "Empty"
        testpart=$(sudo fdisk -l /dev/$name | grep Empty | tail -1 | awk '{print $1}' | sed 's@.*/@@')
        if [ -z "$testpart" ]; then
            # If not, assume we're on m400 where free space is not partitioned automatically
            # So we create a new partition using the free space on the disk
            # NOTE: This has been tested on all currently relevant machines in
            # Cloudlab Utah, Cloudlab Wisconsin, and Cloudlab Clemson
            # However, this assumption might not hold through future machine types 
            oldparts=($(sudo fdisk -l /dev/$name | grep -v Disk | grep $name | awk '{print $1}' | sed 's@.*/@@'))
            sudo apt-get install gdisk -y
            sudo sgdisk -n 0:0:0 /dev/$name
            sudo partprobe
            newparts=($(sudo fdisk -l /dev/$name | grep -v Disk | grep $name | awk '{print $1}' | sed 's@.*/@@'))
            testpart=$(echo ${oldparts[@]} ${newparts[@]} | tr ' ' '\n' | sort | uniq -u)
        fi
        testdevs+=($testpart)
    else
        # Otherwise, if it has no partitions we can do with the disk as we please
        testdevs+=($name)
    fi
done

# Iterate again over the raw block device names to generate disk_info files
for name in "${rawnames[@]}"
do
    filename="disk_info_${name}.csv"
    disk_name="/dev/$name"
    disk_model=$(sudo lsblk -d -io MODEL $disk_name | grep -v MODEL | sed -e 's/[[:space:]]*$//')
    if [ -z "$disk_model" ]; then
        disk_model="N/A"
    fi
    disk_serial=$(sudo lsblk -d -io SERIAL $disk_name | grep -v SERIAL | sed -e 's/[[:space:]]*$//')
    if [ -z "$disk_serial" ]; then
        disk_serial="N/A"
    fi
    disk_size=$(sudo lsblk -d -io SIZE $disk_name | grep -v SIZE | sed -e 's/[[:space:]]*$//')
    if [ -z "$disk_size" ]; then
        disk_size="N/A"
    fi
    isrotational=$(sudo lsblk -d -io ROTA $disk_name | grep -v ROTA | sed -e 's/[[:space:]]*$//')
    if [ -z "$isrotational" ]; then
        disk_type="N/A"
    else
        if [ ${isrotational} == 1 ]; then
            disk_type="HDD"
        else
            disk_type="SSD"
        fi
    fi
    nparts=$(sudo fdisk -l $disk_name | grep -v Disk | grep -c $name)
    echo "run_uuid,timestamp,nodeid,nodeuuid,disk_name,disk_model,disk_serial,disk_size,disk_type,npartitions" > $filename
    echo "$run_uuid,$timestamp,$nodeid,$nodeuuid,$disk_name,$disk_model,$disk_serial,$disk_size,$disk_type,$nparts" >> $filename
done

# Iterate over list of devices generated above
# Run multiple fio commands targeting each
for device in "${testdevs[@]}"
do
    disk="/dev/$device"
    
    # Sequential Write
    rw="write"
    name="fio_write_seq_io${iodepth}_${device}"
    output="$name.csv"
    sudo blkdiscard $disk
    sudo fio --name=$rw --filename=$disk --bs=$blocksize --size=$size --runtime=$timeout --iodepth=$iodepth --direct=$direct --numjobs=$numjobs --ioengine=$ioengine --rw=$rw --minimal --output=$output
    sed -i "1s@\$@;$disk;$iodepth@" $output
    name="fio_write_seq_io1_${device}"
    output="$name.csv"
    sudo blkdiscard $disk
    sudo fio --name=$rw --filename=$disk --bs=$blocksize --size=$size --runtime=$timeout --iodepth=1 --direct=$direct --numjobs=$numjobs --ioengine=$ioengine --rw=$rw --minimal --output=$output
    sed -i "1s@\$@;$disk;1@" $output

    # Random Write
    rw="randwrite"
    name="fio_write_rand_io${iodepth}_${device}"
    output="$name.csv"
    sudo blkdiscard $disk
    sudo fio --name=$rw --filename=$disk --bs=$blocksize --size=$size --runtime=$timeout --iodepth=$iodepth --direct=$direct --numjobs=$numjobs --ioengine=$ioengine --rw=$rw --minimal --output=$output
    sed -i "1s@\$@;$disk;$iodepth@" $output
    name="fio_write_rand_io1_${device}"
    output="$name.csv"
    sudo blkdiscard $disk
    sudo fio --name=$rw --filename=$disk --bs=$blocksize --size=$size --runtime=$timeout --iodepth=1 --direct=$direct --numjobs=$numjobs --ioengine=$ioengine --rw=$rw --minimal --output=$output
    sed -i "1s@\$@;$disk;1@" $output

    # Sequential Read
    rw="read"
    name="fio_read_seq_io${iodepth}_${device}"
    output="$name.csv"
    sudo fio --name=$rw --filename=$disk --bs=$blocksize --size=$size --runtime=$timeout --iodepth=$iodepth --direct=$direct --numjobs=$numjobs --ioengine=$ioengine --rw=$rw --minimal --output=$output
    sed -i "1s@\$@;$disk;$iodepth@" $output
    name="fio_read_seq_io1_${device}"
    output="$name.csv"
    sudo fio --name=$rw --filename=$disk --bs=$blocksize --size=$size --runtime=$timeout --iodepth=1 --direct=$direct --numjobs=$numjobs --ioengine=$ioengine --rw=$rw --minimal --output=$output
    sed -i "1s@\$@;$disk;1@" $output

    # Random Read
    rw="randread"
    name="fio_read_rand_io${iodepth}_${device}"
    output="$name.csv"
    sudo fio --name=$rw --filename=$disk --bs=$blocksize --size=$size --runtime=$timeout --iodepth=$iodepth --direct=$direct --numjobs=$numjobs --ioengine=$ioengine --rw=$rw --minimal --output=$output
    sed -i "1s@\$@;$disk;$iodepth@" $output
    name="fio_read_rand_io1_${device}"
    output="$name.csv"
    sudo fio --name=$rw --filename=$disk --bs=$blocksize --size=$size --runtime=$timeout --iodepth=1 --direct=$direct --numjobs=$numjobs --ioengine=$ioengine --rw=$rw --minimal --output=$output
    sed -i "1s@\$@;$disk;1@" $output
done
output="fio_*"
sed -i 's/\;/\,/g' $output
sed -i "1s/^/$fioheader/" $output
output="fio_info.csv"
echo "run_uuid,timestamp,nodeid,nodeuuid,fio_version,fio_size,fio_iodepth,fio_direct,fio_numjobs,fio_ioengine,fio_blocksize,fio_timeout" > $output
echo "$run_uuid,$timestamp,$nodeid,$nodeuuid,$fio_version,$size,$iodepth,$direct,$numjobs,$ioengine,$blocksize,$timeout" >> $output

#########################
### Network Bandwidth ###
#########################

# iperf3 client -> server first
sudo apt-get install iperf3 -y
iperf_omit=1
iperf_time=60
timeout=0
max_timeout=360

until iperf3 -V -J -N -O $iperf_omit -t $iperf_time -c $net_server > iperf3_normal.json
do
    sleep 5
    let "timeout+=5"
    if [ "$timeout" -gt "$max_timeout" ]; then
        exit 1
    fi
done

# Probably unnecessary, but this might let other machines have a moment to sneak in
sleep 15

timeout=0
# iperf3 server -> client last
until iperf3 -V -R -J -N -O $iperf_omit -t $iperf_time -c $net_server > iperf3_reversed.json
do
    sleep 5
    let "timeout+=5"
    if [ "$timeout" -gt "$max_timeout" ]; then
        exit 1
    fi
done