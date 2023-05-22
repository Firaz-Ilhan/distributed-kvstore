# distributed-kvstore

a distributed key-value store in Go

# How to Use

To build and run this project, you need to install Go.

You can run the application with the following command in the terminal:

```shell
go run main.go -port <port> -nodes <comma-separated-list-of-other-nodes>
```

Here is an example configuration with three nodes:

```shell
go run main.go -port 8080 -nodes localhost:8081,localhost:8082
```

```shell
go run main.go -port 8081 -nodes localhost:8080,localhost:8082
```

```shell
go run main.go -port 8082 -nodes localhost:8080,localhost:8081
```

# API

- GET /{key}: Get the value for a key
- PUT /{key}: Set a value for a key. The request body should contain the value
- DELETE /{key}: Delete a key


