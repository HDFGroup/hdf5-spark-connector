#!/bin/bash

ls -R data

JARS=lib/5parky_2.11-0.0.2-BETA.jar,lib/sis-jhdf5-18.09.0-pre1.jar,lib/sis-base-18.08.0.jar

spark-shell -i examples/basics.scala --jars $JARS
