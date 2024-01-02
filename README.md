
# bson-to-json

This application converts Mongodump's bson.gz files into pure json files.

# Running the app

```bash
java -jar bson-to-json.jar mycollection.bson.gz output.json

# argument 1: The bson.gz file to read
# argument 2: The file to write the json into
```

# Design

This application is written in Scala and uses the fs2 streaming library. It's streaming architecture, that includes inline gzip decompression can work with files 10GB with minimal ram requirements.

# Deployment

Java's slogan is "write-once, run anywhere". You can take the jar file and run it on any Java > JDK 17.
