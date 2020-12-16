package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/urfave/cli/v2"
)

const (
	BulkFileExt = ".json"
)

var (
	version             = "0.0.1"
	ErrInvalidDirectory = errors.New("invalid directory")
)

func main() {
	app := &cli.App{
		Name:    "flow-dump2es",
		Usage:   "import to elasticsearch from postgresql dump data",
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "es-addr",
				Value:   "http://localhost:9200",
				Aliases: []string{"e"},
				Usage:   "elasticsearch http api server `URL` address",
			},
			&cli.StringFlag{
				Name:    "dir",
				Value:   "dump",
				Aliases: []string{"d"},
				Usage:   "elasticsearch bulk data `Directory`",
			},
		},
		Action: func(c *cli.Context) error {
			return bulk(c)
		},
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}

}

func bulk(c *cli.Context) error {
	// log
	log.SetPrefix("[flow-dump2es] ")
	// es config
	cfg := elasticsearch.Config{
		Addresses: []string{
			c.String("es-addr"),
		},
		// Retry on 429 TooManyRequests statuses
		RetryOnStatus: []int{502, 503, 504, 429},
		// Retry up to 5 attempts
		MaxRetries: 10,
	}
	// es client
	ec, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return err
	}
	// bulk
	dir := c.String("dir")
	s, err := os.Stat(dir)
	if err != nil || !s.IsDir() {
		return ErrInvalidDirectory
	}
	err = filepath.Walk(dir, walk(ec))
	if err != nil {
		return nil
	}
	_ = ec
	return nil
}

func walk(ec *elasticsearch.Client) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) == BulkFileExt {
			fs, err := os.Open(path)
			if err != nil {
				return err
			}
			defer fs.Close()
			res, err := ec.Bulk(fs)
			if err != nil {
				return nil
			}
			log.Printf(" <- %s\n", path)
			res.Body.Close()
			return nil
		}
		return nil
	}
}
