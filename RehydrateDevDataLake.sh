#!/bin/bash

aws s3 cp s3://data-lake-us-west-2-062519970039/parquet/ s3://pa-dev-datalake/parquet/ --recursive