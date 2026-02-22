#!/bin/bash

prometheus-node-exporter \
    --web.listen-address=":${1}" \
    --collector.disable-defaults \
    --collector.processes \
    --collector.loadavg \
    --collector.cpu \
    --collector.cpufreq \
    --collector.diskstats \
    --collector.ethtool \
    --collector.hwmon \
    --collector.infiniband \
    --collector.interrupts \
    --collector.meminfo \
    --collector.meminfo_numa \
    --collector.netstat \
    --collector.os \
    --collector.pressure \
    --collector.qdisc \
    --collector.sockstat \
    --collector.softnet \
    --collector.stat \
    --collector.tcpstat \
    --collector.timex \
    --collector.vmstat