#!/bin/bash
for i in {2..9}; do ssh  bic0$i 'killall SSmonitor.py'; done
for i in {2..9}; do ssh  bic0$i 'killall SSdaemon.py'; done
for i in {2..9}; do ssh  bic0$i 'killall mg.D.16'; done
for i in {2..9}; do ssh  bic0$i 'killall lu.D.16'; done
for i in {2..9}; do ssh  bic0$i 'killall cg.D.16'; done
for i in {2..9}; do ssh  bic0$i 'killall ep.D.16'; done
for i in {2..9}; do ssh  bic0$i 'killall graph500_reference_bfs'; done
for i in {2..9}; do ssh  bic0$i 'killall h264ref_base.cpu2006.linux64.intel64.fast'; done
for i in {2..9}; do ssh  bic0$i 'killall bwaves_base.cpu2006.linux64.intel64.fast'; done
/home/txc/ssTest/spark/clean.sh
killall SSmaster.py