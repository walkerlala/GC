#!/bin/bash

usage() {
    echo "usage:"
    echo "    ./run.sh manager"
    echo "or"
    echo "    ./run.sh crawler"
    echo "or"
    echo "    sudo ./run.sh stop(terminate) (to terminate managers/crawlers)"
    echo "or"
    echo "    ./run.sh show(status) (to show current process(manager/crawler) info"
}

manager() {
    echo -n "Running manager..."
    rm -f ./log/_blmftr_
    python3 ./src/manager.py > ./log/manager.out 2> ./log/manager.err &
    show
}

crawler() {
    echo -n "Running crawler..."
    python3 ./src/crawler.py > ./log/crawler.out 2> ./log/crawler.err &
    show
}

terminate() {
    echo -n "Killing process..."
    kill -KILL $(ps aux|perl -ne 'print "$1 " if /.*?(\d+).*python3 \.\/src\/(manager|crawler)\.py/') > /dev/null 2>&1
    if [[ "$?" == 0 ]]
    then
        echo "OK"
    else
        echo "FAIL (maybe no process running currently, or you have no permission)"
    fi
}

show() {
    echo ""
    man=$(ps aux|perl -ne 'print "$_" if /python3 \.\/src\/manager\.py/')
    cra=$(ps aux|perl -ne 'print "$_" if /python3 \.\/src\/crawler\.py/')
    if [[ -z $man && -z $cra ]]; then
        echo "FAIL!!"
        echo "No manager/crawler is running in background."
    else
        echo ""
        echo "$man"
        echo "$cra"
    fi
    echo ""
}

st() {
    show
}

# create log dir
mkdir -p log

if [[ $# != 1 ]]
then
    usage
elif [[ "$1" == "manager" ]]
then
    manager
elif [[ "$1" == "crawler" ]]
then
    crawler
elif [[ "$1" == "stop" ]]
then
    terminate
elif [[ "$1" == "terminate" ]]
then
    terminate
elif [[ "$1" == "show" ]]
then
    show
elif [[ "$1" == "status" ]]
then
    st
else
    usage
fi






