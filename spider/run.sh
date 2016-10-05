#!/bin/bash

usage() {
    echo "usage:"
    echo "    ./run.sh manager"
    echo "or"
    echo "    ./run.sh crawler"
    echo "or"
    echo "    sudo ./run.sh stop (to terminate managers/crawlers)"
    echo "or"
    echo "    sudo ./run.sh terminate (to terminate managers/crawlers)"
}

# FIXME relative path is tricky

manager() {
    cd ./src
    ./manager.py > ../log/manager.out 2> ../log/manager.err &
}

crawler() {
    cd ./src
    ./crawler.py > ../log/crawler.out 2> ../log/crawler.err &
}

terminate() {
    kill -KILL $(ps aux|perl -ne 'print "$1 " if /.*?(\d+).*python3 -u \.\/(manager|crawler)\.py/')
}

# creating log dir
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
else
    usage
fi

