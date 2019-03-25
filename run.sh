java -cp build/deps:build/deps/log4j-1.2.17.jar:build/deps/slf4j-log4j12-1.7.26.jar:build/deps/slf4j-api-1.7.26.jar:build/deps/javassist-3.24.1-GA.jar:. \
     -javaagent:build/libs/spark_graph-unspecified.jar \
     Test
