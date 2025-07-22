#!/bin/bash

ssh-keygen -f "~/.ssh/known_hosts" -R "utilityjumphost-100.shore.$1.prod2.com"
ssh utilityjumphost-$1 "-L 127.0.0.1:8081:sdep-$2-mgmt.shore.$1.prod2.com:8081" -t "ping 8.8.8.8"
