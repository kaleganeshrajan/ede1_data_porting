package app

import (
	"log"
	"net/http"

	cr "github.com/brkelkar/common_utils/configreader"
	"github.com/gin-gonic/gin"
)

var (
	r = gin.Default()
)

//StartApplication Entry point of service
func StartApplication(cfg cr.Config) {
	//log.Println("Calling mapsUrls ")
	mapUrls()
	//port := strconv.Itoa(cfg.Server.Port)
	s := &http.Server{
		Addr:           cfg.Server.Host + ":8080",
		Handler:        r,
		MaxHeaderBytes: 1 << 32,
	}

	err := s.ListenAndServe()
	log.Fatal(err)
}
