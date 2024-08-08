#!/bin/bash

SEALEVEL=$1

docker run -it --rm -v $(pwd):/io -v $(pwd)/spark-events:/spark-events spark-submit --jars ./data/h3-4.0.0.jar target/scala-2.12/lab-1_2.12-1.0.jar ${SEALEVEL}
