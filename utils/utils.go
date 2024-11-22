package utils

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
)

var (
	sess               *session.Session = nil
	DWH_CONSTR_DYNAMIC string           = os.Getenv("DWH_CONSTR_DYNAMIC")
	DWH_USERNAME       string           = os.Getenv("DWH_USERNAME")
	DWH_PASSWORD       string           = os.Getenv("DWH_PASSWORD")
	DWH_DB             string           = os.Getenv("DWH_DB")
	BUCKET             string           = os.Getenv("BUCKET")
)

func getTopicName(topicname string, topics *sns.ListTopicsOutput) *string {
	for _, topic := range topics.Topics {
		topicNameParts := strings.Split(*topic.TopicArn, ":")

		if topicNameParts[len(topicNameParts)-1] == topicname {
			return *&topic.TopicArn
		}
	}
	return nil
}

func SendEmailNotification(msg *string, tN string) {

	svc := sns.New(sess)

	result, err := svc.ListTopics(nil)
	if err != nil {
		log.Println("No topics to list")
		panic(err)
	}
	topicName := getTopicName(tN, result)

	log.Println(result.Topics)
	log.Println("Topicname -> " + *topicName)
	log.Printf("Length -> %d", len(result.Topics))
	output, err := svc.Publish(&sns.PublishInput{
		TopicArn: topicName,
		Message:  msg,
		Subject:  aws.String("BPE PaymentplanAgreement"),
	})
	if err != nil {
		log.Println("Could not publish notification...")
		log.Println(err)
	}

	log.Println(*output.MessageId)

}
func UploadFile(data string, fileName string, bucketName string) error {
	log.Printf("Upload file Recived -> %s\r\n", data)
	svc := s3.New(sess)
	f, err := os.Create(fmt.Sprintf("/tmp/%s", fileName))

	if err != nil {
		log.Println("Error opening file")
		log.Println(err)
		return err
	}
	defer f.Close()

	bytelen, err := f.WriteString(data)

	if err != nil {
		log.Println("Error writing to file")
		log.Println(err)
		return err
	}

	log.Printf("Wrote %d bytes to file \r\n", bytelen)

	log.Println("Resetting file before upload")

	f.Seek(0, io.SeekStart)
	op, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Body:   f,
		Key:    aws.String(fileName),
	})

	if err != nil {

		log.Println("Put object didnt work")
		log.Println(err)
		return err
	}
	log.Println(op.String())
	return nil

}

func CreateSession() *session.Session {
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	return sess
}

func InitDB() (*sql.DB, error) {
	constr := fmt.Sprintf(DWH_CONSTR_DYNAMIC, DWH_USERNAME, DWH_PASSWORD, DWH_DB)
	DB, err := sql.Open("mssql", constr)
	if err != nil {
		log.Println("Could not connect")
		return nil, err
	}
	return DB, nil
}

func GetFileS3(fileName string) (string, error) {
	log.Println("Trying to get a file")
	log.Printf("FileName=%v\r\n", fileName)
	svc := s3.New(sess)
	f, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(fileName),
	})

	if err != nil {
		log.Println("Could not get file")
		log.Println(err)
		return "", err
	}

	bytes, e := io.ReadAll(f.Body)

	if e != nil {
		log.Println("Could not read file")
		return "", e
	}

	return string(bytes), nil

}

func SetLocationGlobal() (*time.Location, error) {
	var timeZone string = "Europe/Oslo"
	loc, err := time.LoadLocation(timeZone)
	if err != nil {
		log.Printf("Unable to load locale %s\r\n", timeZone)
		return nil, err
	} else {
		time.Local = loc

	}
	return loc, nil
}
