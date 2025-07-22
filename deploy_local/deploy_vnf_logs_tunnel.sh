#!/bin/bash

cicd sshme --extra-args "-R $3:127.0.0.1:$2 -t apk add socat; socat tcp-l:$2,fork,reuseaddr tcp:127.0.0.1:$3; bash -l" $1
