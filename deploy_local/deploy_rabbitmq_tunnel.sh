#!/bin/bash

cicd sshme --extra-args "-L 127.0.0.1:5672:127.0.0.1:5672 -L 127.0.0.1:15672:127.0.0.1:15672 ping 8.8.8.8; bash -l" $1
