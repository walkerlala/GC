#!/bin/bash

usage() {
    echo "usage:"
    echo "    ./run.sh manager"
    echo "or"
    echo "    ./run.sh crawler"
}

manager() {
    cd ./src
    ./manager.py > ../log/manager.out 2> ../log/manager.err &
}

crawler() {
    cd ./src
    ./crawler.py > ../log/crawler.out 2> ../log/crawler.err &
}

if [[ $# != 1 ]]
then
    usage
elif [[ "$1" == "manager" ]]
then
    manager
elif [[ "$1" == "crawler" ]]
then
    crawler
else
    usage
fi

