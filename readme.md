# Running the Project

```shell
$ docker build -t kafka-topics
$ docker run --rm --name kafka -v $(pwd):/go/src -v /go/src/tmp -e AIR_TMP_DIR=/go/src/tmp kafka-topics
```
