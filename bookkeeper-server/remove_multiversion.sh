#!/bin/bash

JAR_PATH="/home/fmasci/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.17.1/jackson-core-2.17.1.jar"
TMP_DIR="$(mktemp -d)"

echo "Estraggo il jar in $TMP_DIR ..."
cd "$TMP_DIR" || exit 1
jar xf "$JAR_PATH"

echo "Rimuovo la cartella META-INF/versions ..."
rm -rf META-INF/versions

echo "Rimuovo la proprietà Multi-Release dal MANIFEST ..."
sed -i '/Multi-Release/d' META-INF/MANIFEST.MF

echo "Ricreo il jar pulito..."
jar cf "$JAR_PATH" .

echo "Pulizia temporanei..."
cd - >/dev/null
rm -rf "$TMP_DIR"

echo "Fatto: jar multi-release pulito e sostituito in $JAR_PATH"
