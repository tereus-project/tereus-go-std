package s3

import (
	"context"
	"io"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/tags"
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

func (s *S3Service) RemoveObject(path string) error {
	return s.client.RemoveObject(context.Background(), s.bucket, path, minio.RemoveObjectOptions{})
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

func (s *S3Service) RemoveObjects(prefix string) error {
	objects := s.GetObjects(prefix)

	for object := range objects {
		if object.Err != nil {
			return object.Err
		}

		err := s.RemoveObject(object.Path)
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
func (s *S3Service) ScheduleForDeletion(prefix string) error {
	tagMap := map[string]string{
		"to-delete": "true",
	}
	tags, err := tags.MapToObjectTags(tagMap)
	if err != nil {
		log.Fatalln(err)
	}

	for object := range s.GetObjects(prefix) {
		err := s.client.PutObjectTagging(context.Background(), s.bucket, object.Path, tags, minio.PutObjectTaggingOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}
