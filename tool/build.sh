#!/bin/sh

go mod init tool
go mod tidy
go build .
