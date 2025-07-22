#!/bin/bash

cicd sshme --extra-args "-L 127.0.0.1:8000:10.0.3.9:8000 ping 8.8.8.8; bash -l" $1
