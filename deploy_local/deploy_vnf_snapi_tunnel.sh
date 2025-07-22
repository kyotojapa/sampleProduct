#!/bin/bash

cicd sshme --extra-args "-L 0.0.0.0:8081:10.0.3.9:8081 ping 8.8.8.8; bash -l" $1
