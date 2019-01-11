#!/usr/bin/env bash

curl http://127.0.0.1:8081/get?key=k1

curl http://127.0.0.1:8081/set?key=k1&value=v1

curl http://127.0.0.1:8081/delete?key=k1