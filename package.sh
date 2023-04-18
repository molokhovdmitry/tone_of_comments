#!/bin/bash

rm packages.zip

zip -r packages.zip \
spanemo/__init__.py \
spanemo/model.py \
spanemo/inference.py \
spanemo/data_loader.py \
spanemo/load_model.py \
kafka/__init__.py \
kafka/comments.proto \
kafka/comments_pb2.py