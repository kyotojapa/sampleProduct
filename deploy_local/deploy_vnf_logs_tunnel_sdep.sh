#!/bin/bash

ssh utilityjumphost-dev -R $3:127.0.0.1:$2 -t "sudo apt install socat; killall socat; socat tcp-l:$2,fork,reuseaddr tcp:127.0.0.1:$3"