package s3

import (
	"context"
	"io"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
	"github.com/sirupsen/logrus"
)

type S3Service struct {
	bucket string

	client *minio.Client
}

func NewS3Service(endpoint string, accessKey string, secretKey string, bucket string, secure bool) (*S3Service, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}

	return &S3Service{
		bucket: bucket,
		client: client,
	}, nil
}

func (s *S3Service) MakeBucketIfNotExists(name string) error {
	exists, err := s.client.BucketExists(context.Background(), name)
	if err != nil {
		log.Fatalln(err)
	}

	if !exists {
		err = s.client.MakeBucket(context.Background(), name, minio.MakeBucketOptions{})
		if err != nil {
			// Check to see if we already own this bucket (which happens if you run this twice)
			return err
		}
	}

	return nil
}

func (s *S3Service) PutObject(path string, reader io.Reader, size int64) (info minio.UploadInfo, err error) {
	return s.client.PutObject(
		context.Background(),
		s.bucket,
		path,
		reader,
		size,
		minio.PutObjectOptions{},
	)
}

func (s *S3Service) GetObject(path string) (*minio.Object, error) {
	return s.client.GetObject(context.Background(), s.bucket, path, minio.GetObjectOptions{})
}

type GetObjectsResult struct {
	Err  error
	Path string
	Size int64
}

func (s *S3Service) GetObjects(prefix string) <-chan *GetObjectsResult {
	ch := make(chan *GetObjectsResult)

	go func() {
		defer close(ch)

		objects := s.client.ListObjects(context.Background(), s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})

		for object := range objects {
			ch <- &GetObjectsResult{
				Err:  object.Err,
				Path: object.Key,
				Size: object.Size,
			}
		}
	}()

	return ch
}

func (s *S3Service) ListSubmissionFiles(submissionID string) <-chan *GetObjectsResult {
	ch := make(chan *GetObjectsResult)

	go func() {
		defer close(ch)
		for _, path := range []string{"transpilations/", "transpilations-results/"} {

			objects := s.client.ListObjects(context.Background(), s.bucket, minio.ListObjectsOptions{Prefix: path + submissionID, Recursive: true})

			for object := range objects {
				ch <- &GetObjectsResult{
					Err:  object.Err,
					Path: object.Key,
					Size: object.Size,
				}
			}
		}
	}()

	return ch
}

func (s *S3Service) DeleteSubmission(id string) error {
	logrus.WithField("id", id).Debug("Deleting submission from S3")
	for _, path := range []string{"transpilations/", "transpilations-results/"} {
		objects := s.client.ListObjects(context.Background(), s.bucket, minio.ListObjectsOptions{Prefix: path + id, Recursive: true})

		for object := range objects {
			err := s.client.RemoveObject(context.Background(), s.bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				return err
			}
		}

		err := s.client.RemoveObject(context.Background(), s.bucket, path+id, minio.RemoveObjectOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *S3Service) SizeofObjects(prefix string) int64 {
	size := int64(0)

	for object := range s.GetObjects(prefix) {
		size += object.Size
	}

	return size
}

// Set a tag for the objects to be deleted by Lifecycle later on
func (s *S3Service) ScheduleForDeletion(id string) error {
	tagMap := map[string]string{
		"to-delete": "true",
	}
	tags, err := tags.MapToObjectTags(tagMap)
	if err != nil {
		log.Fatalln(err)
	}

	for _, path := range []string{"transpilations/", "transpilations-results/"} {
		for object := range s.GetObjects(path + id) {
			logrus.WithFields(logrus.Fields{
				"path": object.Path,
				"tags": tags,
			}).Debug("Setting tags on object")

			err := s.client.PutObjectTagging(context.Background(), s.bucket, object.Path, tags, minio.PutObjectTaggingOptions{})
			if err != nil {
				return err
			}
		}
	}

	return nil
}
