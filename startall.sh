#!/bin/bash
SSPATH="/home/txc/SSprototype"
SPARKDIR="/home/txc/spark-2.2.2/sbin/"
cd $SSPATH
for JS in `cat sequences.txt`; do
    for AL in 'CE' 'CS' 'SS'; do
        #for alpha in '0' '0.3' '0.5' '0.7' '0.8' '0.85' '0.9' '0.95' '1.0'; do
        for alpha in '0.9'; do
            echo "AL=$AL, JS=$JS, alpha=$alpha"
            ./SSmaster.py $AL $JS $alpha &
            masterPID=$!

            # shutdown spark masters
            for worker in bic02 bic03 bic04 bic05 bic06 bic07 bic08 bic09
            #for worker in bic03 bic04 bic06 bic07
            #for worker in bic06 bic07
            #for worker in bic06
            do
                ssh -f $worker "cd $SPARKDIR && ./start-master.sh" 
            done
            sleep 2
            echo "Masters started"

            for worker in bic02 bic03 bic04 bic05 bic06 bic07 bic08 bic09
            #for worker in bic03 bic04 bic06 bic07
            #for worker in bic06 bic07
            #for worker in bic06
            do
                # launch SSdaemon
                ssh -f $worker "cd $SSPATH && ./SSdaemon.py"
            done
            wait $masterPID

            # shutdown spark masters
            for worker in bic02 bic03 bic04 bic05 bic06 bic07 bic08 bic09
            #for worker in bic03 bic04 bic06 bic07
            #for worker in bic06 bic07
            #for worker in bic06
            do
                ssh -f $worker "cd $SPARKDIR && ./stop-master.sh" 
                ssh -f $worker "cd $SPARKDIR && ./stop-slave.sh" 
            done

            sleep 180
        done
    done
done
