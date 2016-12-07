package s3

import (
	"compress/gzip"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/elastic/beats/libbeat/logp"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	//"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	//"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const managerMaxFiles = 1024
const defaultKeepFiles = 7
const defaultUploadEveryBytes = 10 * 1024 * 1024
const defaultUploadEverySeconds = 0

type fileManager struct {
	Path               string
	Name               string
	Region             string
	Bucket             string
	UploadEveryBytes   *uint64
	UploadEverySeconds *int64
	KeepFiles          *int

	current      *os.File
	current_size uint64
	last         string
}

func (manager *fileManager) createDirectory() error {
	fileinfo, err := os.Stat(manager.Path)
	if err == nil {
		if !fileinfo.IsDir() {
			return fmt.Errorf("S3 %s exists but it's not a directory", manager.Path)
		}
	}

	if os.IsNotExist(err) {
		err = os.MkdirAll(manager.Path, 0755)
		if err != nil {
			return err
		}
	}

	return nil
}

func (manager *fileManager) checkIfConfigSane() error {
	if len(manager.Name) == 0 {
		return fmt.Errorf("S3 logging requires a name for the file names")
	}
	if len(manager.Bucket) == 0 {
		return fmt.Errorf("S3 logging requires a bucket name")
	}
	if manager.KeepFiles == nil {
		manager.KeepFiles = new(int)
		*manager.KeepFiles = defaultKeepFiles
	}
	if manager.UploadEveryBytes == nil {
		manager.UploadEveryBytes = new(uint64)
		*manager.UploadEveryBytes = defaultUploadEveryBytes
	}
	if manager.UploadEverySeconds == nil {
		manager.UploadEverySeconds = new(int64)
		*manager.UploadEverySeconds = defaultUploadEverySeconds
	}

	if *manager.KeepFiles < 2 || *manager.KeepFiles >= managerMaxFiles {
		return fmt.Errorf("S3 number of files to keep should be between 2 and %d", managerMaxFiles-1)
	}
	return nil
}

func (manager *fileManager) writeLine(line []byte) error {
	if manager.shouldRotate() {
		err := manager.rotate()
		if err != nil {
			return err
		}
	}

	line = append(line, '\n')
	_, err := manager.current.Write(line)
	if err != nil {
		return err
	}
	manager.current_size += uint64(len(line))

	return nil
}

func (manager *fileManager) shouldRotate() bool {
	if manager.current == nil {
		logp.Info("S3 rotate: current nil")
		return true
	}

	if manager.current_size >= *manager.UploadEveryBytes {
		logp.Info("S3 rotate: current size %v > %v", manager.current_size, *manager.UploadEveryBytes)
		return true
	}

	// mtime of dot-1 file should equate to last upload time
	if *manager.UploadEverySeconds > 0 {
		file_path := manager.filePath(1)
		fileInfo, err := os.Lstat(file_path)
		if err != nil {
			logp.Info("S3 could not stat: $s\n", err)
		}

		tMtime := fileInfo.ModTime()
		tNow := time.Now()

		var tDiff int64
		tDiff = tNow.Sub(tMtime).Nanoseconds() / 1000000000

		//logp.Info("S3 time diff on %v: %v seconds", file_path, tDiff)

		if tDiff >= *manager.UploadEverySeconds {
			logp.Info("S3 rotate: mtime diff %v > %v", tDiff, *manager.UploadEverySeconds)
			return true
		}
	}

	return false
}

func (manager *fileManager) localIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func (manager *fileManager) s3KeyName() string {
	// Discern hostname or IP address
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}

	if host == "" || host == "localhost" {
		host = manager.localIP()
	}

	// could still be empty string so could be random fallback
	if host == "" {
		host = "localhost"
	}

	t := time.Now().UTC()

	timeIso8601 := fmt.Sprintf("%d%02d%02dT%02d%02d%02d.%09dZ",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(),
		t.Nanosecond())

	// Final format is /YYYY/MM/DD/HOST_ISO8601
	keyName := fmt.Sprintf("/%d/%02d/%02d/%s_%s",
		t.Year(), t.Month(), t.Day(),
		host, timeIso8601)

	return keyName
}

func (manager *fileManager) s3Upload() error {
	logp.Info("S3 upload path: %v", manager.last)

	file, err := os.Open(manager.last)
	if err != nil {
		logp.Info("S3 err opening file: %s\n", err)
	}
	defer file.Close()

	// compress
	reader, writer := io.Pipe()
	go func() {
		gw := gzip.NewWriter(writer)
		io.Copy(gw, file)
		file.Close()
		gw.Close()
		writer.Close()
	}()

	// aws session
	cfg := aws.NewConfig().WithRegion(manager.Region)
	sess, err := session.NewSession(cfg)
	if err != nil {
		logp.Info("S3 failed to create session: %v", err)
		return err
	}

	// upload
	key := manager.s3KeyName() + ".gz"

	params := &s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(manager.Bucket),
		Key:    aws.String(key),
	}

	uploader := s3manager.NewUploader(sess)
	result, err := uploader.Upload(params)
	if err != nil {
		logp.Info("S3 upload failure: %v", err)
	}

	logp.Info("S3 upload success: %v", result.Location)

	return nil
}

func (manager *fileManager) filePath(file_no int) string {
	if file_no == 0 {
		return filepath.Join(manager.Path, manager.Name)
	}
	filename := strings.Join([]string{manager.Name, strconv.Itoa(file_no)}, ".")
	return filepath.Join(manager.Path, filename)
}

func (manager *fileManager) fileExists(file_no int) bool {
	file_path := manager.filePath(file_no)
	_, err := os.Stat(file_path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (manager *fileManager) rotate() error {

	if manager.current != nil {
		if err := manager.current.Close(); err != nil {
			return err
		}
	}

	// delete any extra files, normally we shouldn't have any
	for file_no := *manager.KeepFiles; file_no < managerMaxFiles; file_no++ {
		if manager.fileExists(file_no) {
			perr := os.Remove(manager.filePath(file_no))
			if perr != nil {
				return perr
			}
		}
	}

	// shift all files from last to first
	for fileNo := *manager.KeepFiles - 1; fileNo >= 0; fileNo-- {
		if !manager.fileExists(fileNo) {
			// file doesn't exist, don't rotate
			continue
		}
		file_path := manager.filePath(fileNo)

		if manager.fileExists(fileNo + 1) {
			// next file exists, something is strange
			return fmt.Errorf("S3 file %s exists when rotating would overwrite it", manager.filePath(fileNo+1))
		}

		err := os.Rename(file_path, manager.filePath(fileNo+1))
		if err != nil {
			return err
		}
	}

	// create the new file
	file_path := manager.filePath(0)
	current, err := os.Create(file_path)
	if err != nil {
		return err
	}
	manager.current = current
	manager.current_size = 0

	// delete the extra file, ignore errors here
	file_path = manager.filePath(*manager.KeepFiles)
	os.Remove(file_path)

	// upload the dot-1 file
	file_path = manager.filePath(1)
	manager.last = file_path
	manager.s3Upload()

	return nil
}
