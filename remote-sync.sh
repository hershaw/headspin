#!/bin/bash

while [ 1 -eq 1 ];
do
    rsync -v --exclude=.git -az headspin sam@172.50.10.30:~/;
done
