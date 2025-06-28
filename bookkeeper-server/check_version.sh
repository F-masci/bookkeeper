#!/bin/bash

CLASSPATH=(
  "/home/fmasci/progetti_uni/isw2/testing/bookkeeper/bookkeeper-server/target/classes"
  "/home/fmasci/.m2/repository/org/apache/bookkeeper/bookkeeper-common/4.18.0-SNAPSHOT/bookkeeper-common-4.18.0-SNAPSHOT.jar"
  "/home/fmasci/.m2/repository/org/apache/bookkeeper/stats/bookkeeper-stats-api/4.18.0-SNAPSHOT/bookkeeper-stats-api-4.18.0-SNAPSHOT.jar"
  "/home/fmasci/.m2/repository/org/apache/bookkeeper/cpu-affinity/4.18.0-SNAPSHOT/cpu-affinity-4.18.0-SNAPSHOT.jar"
  "/home/fmasci/.m2/repository/com/google/guava/guava/32.0.1-jre/guava-32.0.1-jre.jar"
  "/home/fmasci/.m2/repository/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar"
  "/home/fmasci/.m2/repository/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar"
  "/home/fmasci/.m2/repository/org/checkerframework/checker-qual/3.33.0/checker-qual-3.33.0.jar"
  "/home/fmasci/.m2/repository/com/google/errorprone/error_prone_annotations/2.9.0/error_prone_annotations-2.9.0.jar"
  "/home/fmasci/.m2/repository/com/google/j2objc/j2objc-annotations/2.8/j2objc-annotations-2.8.jar"
  "/home/fmasci/.m2/repository/io/netty/netty-common/4.1.121.Final/netty-common-4.1.121.Final.jar"
  "/home/fmasci/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.17.1/jackson-core-2.17.1.jar"
)

for entry in "${CLASSPATH[@]}"
do
  if [[ -d "$entry" ]]; then
    # è una cartella di classi
    CLASSFILE=$(find "$entry" -name '*.class' | head -1)
    echo "Directory $entry, checking $CLASSFILE"
    javap -verbose "$CLASSFILE" | grep "major"
  elif [[ -f "$entry" && "$entry" == *.jar ]]; then
    # è un jar
    CLASSFILE=$(unzip -l "$entry" | grep '\.class$' | head -1 | awk '{print $4}')
    echo "JAR $entry, checking $CLASSFILE"
    unzip -p "$entry" "$CLASSFILE" > /tmp/temp.class
    javap -verbose /tmp/temp.class | grep "major"
    rm /tmp/temp.class
  else
    echo "Entry $entry non trovata o non valida"
  fi
done
