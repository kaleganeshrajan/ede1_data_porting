package utils

import (
	"context"
	"ede_porting/models"
	"log"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

//GcsFile gcs file attributes
type GcsFile struct {
	FileName        string
	FilePath        string
	BucketName      string
	DistributorCode string
	LastUpdateTime  time.Time
	ProcessingTime  string
	Records         int
	FileType        string
	FileSize        int
	ErrorMsg        string
	Source          string
	GcsClient       *GcsBucketClient
	TimeDiffrence   int64
	FileKey         string
}

//HandleGCSEvent  parse file name and set all required attributes for the file
func (g *GcsFile) HandleGCSEvent(ctx context.Context, e models.GCSEvent) *GcsFile {
	var gcsObj GcsBucketClient
	g.GcsClient = gcsObj.InitClient(ctx).SetBucketName(e.Bucket).SetNewReader(e.Name)

	if !g.GcsClient.GetLastStatus() {
		log.Print("Error while reading file")
	}
	g.FileSize, _ = strconv.Atoi(e.Size)
	g.FilePath = e.Bucket + "/" + e.Name
	g.FileName = e.Name
	g.BucketName = e.Bucket
	fileName := strings.Split(e.Name, "/")
	g.FileKey = fileName[len(fileName)-2]
	g.LastUpdateTime = e.Updated

	g.ProcessingTime = e.Updated.Format("2006-01-02")
	return g
}

//LogFileDetails file details logger
func (g *GcsFile) LogFileDetails(status bool) {
	log.Println("CF", zap.String("distributor_code", g.DistributorCode),
		zap.String("FileName", g.FileName),
		zap.Int("FileSize", g.FileSize),
		zap.String("FileType", g.FileType),
		zap.String("ProcessingTime", g.ProcessingTime),
		zap.Bool("Proting_status", status),
		zap.Int64("TimeDiffrence", g.TimeDiffrence),
		zap.Int("record_count", g.Records))
}
