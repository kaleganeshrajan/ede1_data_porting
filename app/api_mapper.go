package app

import "ede_porting/controllers"

func mapUrls() {
	syncApis := r.Group("/api")
	{
		syncApis.POST("/ede_porting", controllers.FileParse)
	}
}
