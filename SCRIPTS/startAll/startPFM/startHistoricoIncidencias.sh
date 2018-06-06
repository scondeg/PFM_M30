#!/bin/bash

/opt/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --verbose --class main.scala.historical.IncidenciasHistoricoAnualBatch --master local[*] /home/pfm/IdeaProjects/PFM_M30/target/PFM_M30-1.0-SNAPSHOT.jar
