# -Dlog.level=DEBUG
# -Dsandbox=false
# -Dfilter_sandbox_tests=true
# -Dsandbox_mode=OFF
# -Dtest_dir=/home/fmasci/bookkeeper-testing/bookkeeper-server/evosuite-tests \
# -Dcores=16
# -Dfunctional_mocking_percent=1
# -Duse_separate_classloader=false

java -jar /home/fmasci/Programmi/evosuite-1.0.6.jar \
      -class org.apache.bookkeeper.bookie.JournalChannel \
      -projectCP target/classes:$(cat cp.txt) \
      -Dsandbox=false \
      -Dsearch_budget=60 \
      -Duse_separate_classloader=false