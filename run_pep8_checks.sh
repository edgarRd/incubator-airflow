#!/usr/bin/env bash

flake8 --show-source airflow/
if [ $? -eq 1 ]
then
  echo 'Failing due to PEP8 errors.'
  exit 1
else
  echo 'PEP8 checks passed.'
fi
