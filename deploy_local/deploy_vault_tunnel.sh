#!/bin/bash

cicd sshme --extra-args "-L 8888:10.0.3.1:8200 ping 8.8.8.8; bash -l" $1
