package main

import (
	"archive/tar"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-faster/errors"
	"github.com/google/go-github/v43/github"
	"github.com/klauspost/compress/gzip"
	"golang.org/x/oauth2"
)

func run(ctx context.Context, tag string) error {
	fmt.Println("Searching for", tag, "release")

	// Using GitHub token if available.
	hClient := http.DefaultClient
	if tok := os.Getenv("GITHUB_TOKEN"); tok != "" {
		fmt.Println("Using GitHub token")
		ts := oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: tok},
		)
		hClient = oauth2.NewClient(ctx, ts)
	} else {
		fmt.Println("No GitHub token found")
	}

	gh := github.NewClient(hClient)
	release, _, err := gh.Repositories.GetReleaseByTag(ctx, "ClickHouse", "ClickHouse", tag)
	if err != nil {
		return errors.Wrapf(err, "get release by tag %s", tag)
	}
	var u string
	for _, a := range release.Assets {
		if !strings.HasPrefix(*a.Name, "clickhouse-common-static-") {
			continue
		}
		if filepath.Ext(*a.Name) != ".tgz" {
			continue
		}
		fmt.Println("Found", *a.Name)
		u = *a.BrowserDownloadURL
		break
	}
	if u == "" {
		return errors.Errorf("asset not found for %s", tag)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return errors.Wrap(err, "new request")
	}

	res, err := hClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "do http")
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode != http.StatusOK {
		return errors.Errorf("bad status %s", res.Status)
	}
	r, err := gzip.NewReader(res.Body)
	if err != nil {
		return errors.Wrap(err, "gzip")
	}
	defer func() {
		_ = r.Close()
	}()
	fmt.Println("Downloading")
	tr := tar.NewReader(r)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return errors.New("file not found in archive")
		}
		if err != nil {
			return errors.Wrap(err, "tar")
		}
		if strings.HasSuffix(h.Name, "/bin/clickhouse") {
			fmt.Println("Found file", h.Name)
			break
		}
	}

	if err := os.MkdirAll("/opt/ch/", 0o777); err != nil {
		return errors.Wrap(err, "mkdir")
	}
	f, err := os.OpenFile("/opt/ch/clickhouse", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o777)
	if err != nil {
		return errors.Wrap(err, "create")
	}
	defer func() {
		_ = f.Close()
	}()
	if _, err := io.Copy(f, tr); err != nil {
		return errors.Wrap(err, "save file")
	}
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "close file")
	}

	fmt.Println("Done")

	return nil
}

func main() {
	flag.Parse()
	ctx := context.Background()
	if err := run(ctx, flag.Arg(0)); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed: %+v\n", err)
		os.Exit(2)
	}

	// https://github.com/ClickHouse/ClickHouse/releases/download/v${{ matrix.clickhouse }}/clickhouse-common-static-${{ steps.asset.outputs.version }}.tgz
	// 22.3.3.44-lts
}
