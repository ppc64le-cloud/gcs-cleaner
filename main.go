package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/storage"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v2"
)

// Config struct for Directory, Bucket and GCS credential path details
type Config struct {
	Dir       string `yaml:"dir"`
	DaysLimit int    `yaml:"daysLimit"`
}

func newConfig(configFile string) ([]Config, error) {
	// Create config structure
	var config []Config
	// Open config file
	file, err := os.Open(configFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// Init new YAML decode
	d := yaml.NewDecoder(file)
	// Start YAML decoding from file
	if err := d.Decode(&config); err != nil {
		return nil, err
	}
	return config, nil
}

func main() {
	var configFile string
	var bucket string
	var gcsCredPath string
	flag.StringVar(&configFile, "config-file", "./config.yml", "Path of the Generic config file for Job details")
	flag.StringVar(&bucket, "bucket", "ppc64le-kubernetes", "GCS Bucket name")
	flag.StringVar(&gcsCredPath, "gcs-cred-path", "/etc/gcs-cred/service-account.json", "Path of GCS service account json")
	flag.Parse()
	cfg, err := newConfig(configFile)
	if err != nil {
		log.Fatal(err)
	}
	for _, job := range cfg {
		if err := listObjectsAndDeleteOlderObjects(bucket, job.Dir, job.DaysLimit, gcsCredPath); err != nil {
			log.Fatalf("listObjectsAndDeleteOlderObjects: %v", err)
		}
	}
}
func listObjectsAndDeleteOlderObjects(bucket string, prefix string, reqDays int, gcsCredPath string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(gcsCredPath))
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*10)
	defer cancel()
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("Bucket(%q).Objects(): %v", bucket, err)
		}
		log.Debug("\nObject Name : ", attrs.Name)
		log.Debug("\nObject Created Time : ", attrs.Created)
		loc, _ := time.LoadLocation("UTC")
		days := int(time.Now().In(loc).Sub(attrs.Created).Hours() / 24)
		log.Debugf("Difference in days : %d days\n", days)
		if days >= reqDays {
			if err := client.Bucket(bucket).Object(attrs.Name).Delete(ctx); err != nil {
				return fmt.Errorf("Object(%q).Delete: %v", attrs.Name, err)
			}
			log.Infof("Object %s Deleted\n", attrs.Name)
		}
	}
	return nil
}
