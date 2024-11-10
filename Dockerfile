FROM golang:1.23

RUN go install github.com/air-verse/air@latest

WORKDIR /go/src
ENV PATH="/go/bin:${PATH}"

RUN apt-get update && \
    apt-get install build-essential librdkafka-dev -y

COPY . .

RUN go mod tidy

CMD ["air"]
