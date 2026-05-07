package main

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed public/*
var publicFiles embed.FS

func publicFileServer() http.Handler {
	subtree, err := fs.Sub(publicFiles, "public")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(subtree))
}
