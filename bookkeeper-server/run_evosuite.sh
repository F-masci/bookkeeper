# -Dlog.level=DEBUG
# -Dsandbox=false
# -Dtest_dir=/home/fmasci/bookkeeper-testing/bookkeeper-server/evosuite-tests \

java -jar /home/fmasci/Programmi/evosuite-1.0.6.jar \
      -class org.apache.bookkeeper.client.LedgerHandle \
      -projectCP target/classes:$(cat cp.txt) \
      -Dsandbox=false \
      -Dsearch_budget=180 \
      -Dcores=16