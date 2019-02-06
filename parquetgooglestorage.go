package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	dt "github.com/trustnetworks/analytics-common/datatypes"
	"github.com/trustnetworks/analytics-common/utils"
	"github.com/trustnetworks/analytics-common/worker"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
)

const pgm = "parquetgooglestorage"

// The queue consists of flat events plus the original event size.
type QueueItem struct {
	event *FlatEvent
	size  int
}

// Flat event queue size
const feQueueSize = 10000

var maxBatch int64
var maxTime float64
var ctx context.Context
var pqwr *Writer
var path string
var uid string
var tm string
var data bytes.Buffer

type work struct {
	client                  *http.Client
	key                     string
	project                 string
	bucketname              string
	bucket                  *storage.Bucket
	svc                     *storage.Service
	basedir                 string
	count                   int64
	items                   int64
	last                    time.Time
	stripPayload            bool
	svcoutageretrysleeptime time.Duration
	feQueue                 chan QueueItem
}

func (s *work) init() error {

	var err error

	s.feQueue = make(chan QueueItem, feQueueSize)

	//defualt file size, max time if no batch size and max time env values set
	var defaultMaxBatch int64 = 268435456
	var defaultMaxTime float64 = 1800
	var mBytes = false
	var kBytes = false

	//get batch size env value and  trim, remove spaces and M or K
	mBatchFromEnv := utils.Getenv("MAX_BATCH", "268435456")
	mBatch := strings.Replace(mBatchFromEnv, "\"", "", -1)
	if strings.Contains(strings.ToUpper(mBatch), "M") {
		mBatch = strings.Replace(strings.ToUpper(mBatch), "M", "", -1)
		mBytes = true
	} else if strings.Contains(strings.ToUpper(mBatch), "K") {
		mBatch = strings.Replace(strings.ToUpper(mBatch), "K", "", -1)
		kBytes = true
	}
	mBatch = strings.Replace(mBatch, " ", "", -1)
	mBatch = strings.TrimSpace(mBatch)

	//check max batch size value set in env is parasable to int if not use default value
	maxBatch, err = strconv.ParseInt(mBatch, 10, 64)
	if err != nil {
		maxBatch = defaultMaxBatch
		utils.Log("Couldn't parse MAX_BATCH: %v :using default %v", mBatchFromEnv, defaultMaxBatch)

	} else {
		if mBytes == true {
			maxBatch = maxBatch * 1024 * 1024
		} else if kBytes == true {
			maxBatch = maxBatch * 1024
		}

	}

	utils.Log("maxBatch set to: %v", maxBatch)

	//get max time env value and  trim, remove spaces
	mTimeFromEnv := utils.Getenv("MAX_TIME", "1800")
	mTime := strings.Replace(mTimeFromEnv, "\"", "", -1)
	mTime = strings.Replace(mTime, " ", "", -1)
	mTime = strings.TrimSpace(mTime)

	//check max time value set in env is parasable to int if not use default value
	maxTime, err = strconv.ParseFloat(mTime, 64)
	if err != nil {
		utils.Log("Couldn't parse MAX_TIME: %v :using default %v", mTimeFromEnv, defaultMaxTime)
		maxTime = defaultMaxTime
	}

	utils.Log("maxTime set to: %v", maxTime)

	s.count = 0
	s.items = 0
	s.last = time.Now()

	s.stripPayload = utils.Getenv("STRIP_PAYLOAD", "false") == "true"

	s.key = utils.Getenv("KEY", "private.json")
	s.project = utils.Getenv("GS_PROJECT", "")
	s.bucketname = utils.Getenv("GS_BUCKET", "")
	s.basedir = utils.Getenv("GS_BASEDIR", "parquet")
	sleepduration, err := strconv.Atoi(utils.Getenv("SVC_OUTAGE_RETRYSLEEPTIME", "10"))
	if err != nil {
		utils.Log("Couldn't get SVC_OUTAGE_RETRYSLEEPTIME: %s,  setting to 10 seconds", err.Error())
		sleepduration = 10
	}
	s.svcoutageretrysleeptime = time.Second * time.Duration(int64(sleepduration))
	utils.Log("svcOutageRetrySleepTime set to: %s", s.svcoutageretrysleeptime)
	//create parquet writer
	pqwr, err = NewWriter(&data)
	if err != nil {
		utils.Log("Couldn't create parquet writer: %s", err.Error())
	}

	key, err := ioutil.ReadFile(s.key)
	if err != nil {
		utils.Log("Couldn't read key file: %s", err.Error())
		return err
	}

	config, err := google.JWTConfigFromJSON(key)
	if err != nil {
		utils.Log("JWTConfigFromJSON: %s", err.Error())
		return err
	}

	config.Scopes = []string{storage.DevstorageReadWriteScope}

	s.client = config.Client(oauth2.NoContext)

	s.svc, err = storage.New(s.client)
	if err != nil {
		utils.Log("Couldn't create client: %s", err.Error())
		return err
	}

	var bucket storage.Bucket
	bucket.Name = s.bucketname
	bucket.Kind = "storage#bucket"

	s.bucket, err = s.svc.Buckets.Insert(s.project, &bucket).Do()
	if err != nil {
		utils.Log("Bucket create failed (ignored): %s", err.Error())
	}

	return nil

}

func (s *work) Handle(msg []uint8, w *worker.Worker) error {

	var e dt.Event

	// Convert JSON object to internal object.
	err := json.Unmarshal(msg, &e)
	if err != nil {
		utils.Log("Couldn't unmarshall json: %s", err.Error())
		return nil
	}

	fl := Flattener{
		WritePayloads: false,
	}

	//flatten json event
	oe := fl.FlattenEvent(&e)

	s.feQueue <- QueueItem{event: oe, size: len(msg)}

	return nil

}

func (s *work) QueueHandler() error {

	for {

		oe := <-s.feQueue
		err := s.HandleQueueItem(oe)
		if err != nil {
			utils.Log("Couldn't process queue item: %s", err.Error())
		}

	}

}

func (s *work) HandleQueueItem(oe QueueItem) error {

	if (s.count > maxBatch) || (time.Since(s.last).Seconds() > maxTime) {

		//create a new bucket storage path
		tm = time.Now().Format("2006-01-02/15-04")
		uid = uuid.New().String()
		path := s.basedir + "/" + tm + "/" + uid + ".parquet"

		var object storage.Object
		object.Name = path
		object.Kind = "storage#object"

		//close parquet writer
		err := pqwr.Close()
		if err != nil {
			utils.Log("Couldn't close parquet writer: %s", err.Error())
		}

		//retry if storage service is intermitent
		for {

			rdr := bytes.NewReader(data.Bytes())

			_, err = s.svc.Objects.Insert(s.bucketname, &object).Media(rdr).Do()
			if err != nil {
				utils.Log("Couldn't insert in to bucket: %s", err.Error())
				utils.Log("qlen=%d", len(s.feQueue))
				time.Sleep(s.svcoutageretrysleeptime)
			} else {
				// exiting for, when there are no storage service connection issues
				break
			}
		}
		//clear buffer for the next batch data
		data.Reset()
		//create new parquet writer
		pqwr, err = NewWriter(&data)

		//reset counter and time
		s.last = time.Now()
		s.count = 0
		s.items = 0

	} else {

		s.count += int64(oe.size)
		s.items += 1

		/*if (s.items % 2500) == 0 {
			utils.Log("items=%d size=%d qlen=%d", s.items, s.count,
				len(s.feQueue))
		}*/

		//convert to parquet format using parquet writer
		err := pqwr.Write(*oe.event)
		if err != nil {
			utils.Log("Couldn't write in to buffer: %s", err.Error())
			return nil
		}
	}

	return nil

}

func (s *work) QueueSizeReporter() {
	for {
		qln := len(s.feQueue)
		if qln > 0 {
			utils.Log("qlen=%d", qln)
		}
		time.Sleep(time.Second * 1)
	}
}

func main() {

	var w worker.QueueWorker
	var s work
	utils.LogPgm = pgm

	utils.Log("Initialising...")

	err := s.init()
	if err != nil {
		utils.Log("init: %s", err.Error())
		return
	}

	var input string
	var output []string

	if len(os.Args) > 0 {
		input = os.Args[1]
	}
	if len(os.Args) > 2 {
		output = os.Args[2:]
	}

	// context to handle control of subroutines
	ctx := context.Background()
	ctx, cancel := utils.ContextWithSigterm(ctx)
	defer cancel()

	err = w.Initialise(ctx, input, output, pgm)
	if err != nil {
		utils.Log("init: %s", err.Error())
		return
	}

	go s.QueueHandler()
	//go s.QueueSizeReporter()

	utils.Log("Initialisation complete.")

	// Invoke Wye event handling.
	err = w.Run(ctx, &s)
	if err != nil {
		utils.Log("error: Event handling failed with err: %s", err.Error())
	}

}
