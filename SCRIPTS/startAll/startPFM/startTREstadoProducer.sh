#!/bin/bash

/opt/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --verbose --class main.scala.producer.EstadoTraficoProducer --master local[*] --driver-memory 1G --executor-memory 1G  /home/pfm/IdeaProjects/PFM_M30/target/PFM_M30-1.0-SNAPSHOT.jar
