
# bson-to-json

This application converts Mongodump's bson.gz files into pure json files. The JSON format matches the "Extended JSON format" documented here: https://www.mongodb.com/docs/manual/reference/mongodb-extended-json-v1/

## Running the app

```bash
java -jar bson-to-json.jar mycollection.bson.gz output.json.gz

# argument 1: The bson.gz file to read
# argument 2: The file to write the json into. (.gz is optional but adds compression)
```

## Design

This application is written in Scala and uses the fs2 streaming library. It's streaming architecture, that includes inline gzip decompression can work with files 10GB+ with minimal ram requirements. 

## Options

If you have a slow machine you can try and tune the JVM `export JAVA_OPTS="-XX:+UseG1GC -Xmx4096m"`.

### Pandas Mode

Normally when converting bson objects to json, we define the collection inside an array like this.

```json
[
   {
    "id" : 1,
    "name": "phill"
   },
   {
    "id" : 2,
    "name": "loretta"
   }
]
```

Since json.loads tries to load the whole array it can make your system run out of memory. Pandas.read_json also has the same problem. To overcome this Pandas.read_json has a `lines=True` mode.

You can pass a `--pandas` argument into this app to make it skip the outer array and to use newlines between each json object, so the output is like this:

```json
{ "id" : 1, "name": "phill" }
{ "id" : 2, "name": "loretta" }
```

This is also helpful when loading the data into elasticsearch as well. This format is sometimes refered to as JSONL.

The other thing that `--pandas` mode does is flatten nested JSON:

```
[
    { "chocolate": { "pudding" : true }},
    { "after" : [ { "eight" : true }, { "eight" : { "check" : false }}}
]
```
would become:

```
{ "chocolate.pudding": true }
{ "after.0.eight" : true, "after.1.eight.check" : false }
```

## Deployment

Java's slogan is "write-once, run anywhere". You can download the jar from the github releases link and run it on any JVM > JDK 17.
