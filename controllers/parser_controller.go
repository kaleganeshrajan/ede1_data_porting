package controllers

import (
	"ede_porting/models"
	"ede_porting/parsers"
	"ede_porting/utils"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	cm models.Common
)

//FileParse Update Stock data
func FileParse(c *gin.Context) {
	var m models.PubSubMessage

	timeNow := time.Now().In(utils.ConvertUTCtoIST())

	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		LogResponceEvent("Ede1: Reading data error: "+err.Error(), http.StatusBadRequest, 0, timeNow)
		c.AbortWithStatus(http.StatusBadRequest)
	}

	if err := json.Unmarshal(body, &m); err != nil {
		LogResponceEvent("Ede1: Error while UnMarshaling error:"+err.Error(), http.StatusBadRequest, 0, timeNow)
		c.AbortWithStatus(http.StatusBadRequest)
	}

	err = parsers.Worker(m)
	if err != nil {
		c.JSON(http.StatusOK, "")
	} else {
		c.JSON(http.StatusOK, "done")
	}
}
