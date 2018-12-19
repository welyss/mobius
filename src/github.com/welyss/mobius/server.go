package main

import (
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	BACKUP_FOR_SNAPSHOT_REQ_ATTR_CLUSTER = "cluster"
	ENV_BACKUP_PORT                      = "BACKUP_PORT"
	ENV_BACKUP_SERVER                    = "BACKUP_SERVER"
	ENV_HTTP_DIR                         = "HTTP_DIR"
	ENV_DISCOVERY_SERVICE                = "DISCOVERY_SERVICE"
	ENV_NCAT_PORT                        = "NCAT_PORT"
	ENV_RW_BUFFER_SIZE                   = "RW_BUFFER_SIZE"
	ENV_READ_TIMEOUT                     = "READ_TIMEOUT"
)

var rwBufSize = os.Getenv(ENV_RW_BUFFER_SIZE)
var httpDir = os.Getenv(ENV_HTTP_DIR)
var discoveryService = os.Getenv(ENV_DISCOVERY_SERVICE)
var backupPort = os.Getenv(ENV_BACKUP_PORT)
var backupServer = os.Getenv(ENV_BACKUP_SERVER)
var ncatPort = os.Getenv(ENV_NCAT_PORT)
var readTimeoutInSecond = os.Getenv(ENV_READ_TIMEOUT)

func init() {
	if httpDir == "" {
		httpDir = "d:/backups"
	}
	if discoveryService == "" {
		//		discoveryService = "etcd.kubernetes:2379"
		discoveryService = "192.168.56.51:2379"
	}
	if backupPort == "" {
		backupPort = "8080"
	}
	if ncatPort == "" {
		ncatPort = "3307"
	}
	if rwBufSize == "" {
		rwBufSize = "67108864"
	}
	if readTimeoutInSecond == "" {
		readTimeoutInSecond = "60"
	}
}

func main() {
	http.Handle("/", http.FileServer(http.Dir(httpDir)))
	http.HandleFunc("/backup.do", backupForSnapshot)
	err := http.ListenAndServe(backupServer+":"+backupPort, nil)
	if err != nil {
		log.Fatal(err)
	}
}

// backup main func
func backupForSnapshot(writer http.ResponseWriter, req *http.Request) {
	var output string
	defer func() {
		if r := recover(); r != nil {
			log.Println(r)
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("pkg: %v", r)
				log.Println("recover:", err)
			}
		}
	}()
	log.Println("Start BackupForSnapshot...")
	cluster := req.FormValue(BACKUP_FOR_SNAPSHOT_REQ_ATTR_CLUSTER)
	log.Println("Cluster:", cluster)
	if fromServer, err := findBackupTarget(cluster); err == nil {
		log.Println("Cluster Host Is:", fromServer, ", Port Is:", ncatPort)
		log.Println("Dumping...")
		file, length, err := backupToFile(fromServer+":"+ncatPort, cluster)
		output = "Backup From " + fromServer + " For " + cluster + " To " + file + " Done, Backup State: " + err.Error() + ", File Size: " + strconv.Itoa(length)
	} else {
		output = err.Error()
	}
	log.Println(output)
	fmt.Fprint(writer, output)
}

func getEtcdServer() (server string, err error) {
	for _, server := range strings.Split(discoveryService, ",") {
		url := "http://" + server + "/health"
		if _, err := http.Head(url); err == nil {
			return server, err
		}
	}
	return "", err
}

func findBackupTarget(cluster string) (target string, err error) {
	var etcdServer string
	if etcdServer, err = getEtcdServer(); err == nil {
		var resp *http.Response
		if resp, err = http.Get("http://" + etcdServer + "/v2/keys/mysql/" + cluster + "/nodes"); err == nil && resp.StatusCode == 200 {
			var data []byte
			if data, err = ioutil.ReadAll(resp.Body); err == nil {
				gnodes := gjson.Get(string(data), "node.nodes")
				if gnodes.Exists() {
					nodes := gnodes.Array()
					for i := len(nodes) - 1; i >= 0; i-- {
						if nodes[i].Get("value").String() == "ONLINE" {
							target = nodes[i].Get("key").String()
							target = target[strings.LastIndex(target, "/") + 1:len(target)]
						}
					}
				}
			}
		}
	}
	if target == "" {
		return "", errors.New("No Available Etcd Host.")
	}
	return
}

func backupToFile(from string, cluster string) (file string, length int, err error) {
	length = 0
	var conn net.Conn
	conn, err = net.Dial("tcp", from)
	if err != nil {
		return
	}
	defer conn.Close()
//	readTimeout, err := strconv.Atoi(readTimeoutInSecond)
//	if err != nil {
//		return err
//	}
//	if err = conn.SetReadDeadline(time.Now().Add(time.Second * time.Duration(readTimeout))); err != nil {
//		return err
//	}
	bufSize, _ := strconv.Atoi(rwBufSize)
	rwBuf := make([]byte, bufSize)
	backupDir := httpDir + "/" + cluster
	if err = os.MkdirAll(backupDir, 0644); err != nil {
		return
	}
	now := time.Now()
	file = backupDir + "/" + fmt.Sprintf("%04d-%02d-%02d_%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second()) + ".xbstream"
	var backupFile *os.File
	backupFile, err = os.OpenFile(file , os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer backupFile.Close()
	for {
		var rlength int
		rlength, err = conn.Read(rwBuf)
		if err == nil {
			var wlength int
			wlength, err = backupFile.Write(rwBuf[0:rlength])
			length += wlength
			if err != nil {
				return
			}
		} else {
			return
		}
	}
	return
}
