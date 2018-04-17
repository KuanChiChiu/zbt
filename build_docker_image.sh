#!/bin/sh

# 執行 sh build_docker_image.sh beta beta-1.0.x

cp /root/.ssh/id_rsa ./
cp ./conf/$1_config.yml ./conf/config.yml
docker build --no-cache -t "zbt:$2" .
rm ./id_rsa
