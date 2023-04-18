#!/bin/bash

cd kafka
protoc --python_out=. comments.proto
