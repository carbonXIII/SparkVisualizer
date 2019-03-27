# SparkVisualizer
A simple DAG visualizer for spark using instrumentation and python networkx

## Building:
Just run the gradle script to build the Java agent.
```
gradle build
```

## Dependencies:
Python visualizer:
- networkx
- tkinter
- graphviz (and pygraphviz)

## Running
Run the visualizer itself:
```
python3 visualizer.py
```
Run the desired spark program with the javaagent specified in --driver-java-options
```
spark-shell --driver-java-options "-javaagent:./build/libs/SparkVisualizer-unspecified.jar"
```

