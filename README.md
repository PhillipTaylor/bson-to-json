
# bson-to-json

This application converts Mongodump's bson.gz files into pure json files. The JSON format matches the "Extended JSON format" documented here: https://www.mongodb.com/docs/manual/reference/mongodb-extended-json-v1/

## Running the app

```bash
java -jar bson-to-json.jar mycollection.bson.gz output.json

# argument 1: The bson.gz file to read
# argument 2: The file to write the json into
```

## Design

This application is written in Scala and uses the fs2 streaming library. It's streaming architecture, that includes inline gzip decompression can work with files 10GB+ with minimal ram requirements. If you have a slow machine you can try and tune the JVM `export JAVA_OPTS="-XX:+UseG1GC -Xmx4096m"`.

## Deployment

Java's slogan is "write-once, run anywhere". You can download the jar from the github releases and run it on any JVM > JDK 17.
