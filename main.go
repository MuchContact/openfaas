package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/nats-io/stan.go"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openfaas/faas-provider/auth"
	"github.com/openfaas/faas/gateway/queue"
	"github.com/openfaas/nats-queue-worker/nats"
	//"github.com/openfaas/nats-queue-worker/version"
)

// request /channelsz?sub=1
func requestNatsServer2GetChannelsz() (uint64, uint64) {
	return 1, 1
}

func handler(w http.ResponseWriter, r *http.Request) {
	//counter := uint64(0)
	////atomic.AddUint64(&counter, 1)
	////atomic.AddUint64(&counter, 1)
	//started := time.Now()
	//
	////timeTaken := time.Since(started).Seconds()
	//for counter <= 0 && time.Since(started).Seconds() <= 60 {
	//	time.Sleep(50 * time.Millisecond)
	//}
	//	fmt.Fprintf(w, "Hi there, I love %s %d!", r.URL.Path[1:], counter)

	//var finishFlag bool = false
	counter := uint64(0)

	// request channelsz to get current consume status
	startSeq, endSeq := requestNatsServer2GetChannelsz()
	// start a new consumer to status msgs
	readConfig := ReadConfig{}
	config, configErr := readConfig.Read()
	if configErr != nil {
		panic(configErr)
	}
	hostname, _ := os.Hostname()

	natsURL := fmt.Sprintf("nats://%s:%d", config.NatsAddress, config.NatsPort)
	metricGroupByFunc := make(map[string]*uint64)
	messageHandler := func(msg *stan.Msg) {
		if msg.Sequence >= endSeq {
			atomic.AddUint64(&counter, 1)
			return
		}

		req := queue.Request{}
		unmarshalErr := json.Unmarshal(msg.Data, &req)
		if unmarshalErr != nil {
			log.Printf("Unmarshal error: %s with data %s", unmarshalErr, msg.Data)
			return
		}

		target := metricGroupByFunc[req.Function]
		if target == nil {
			initCounter := uint64(0)
			metricGroupByFunc[req.Function] = &initCounter
		} else {
			atomic.AddUint64(target, 1)
		}
	}
	natsQueue := NATSQueue{
		clusterID: config.NatsClusterName,
		clientID:  "faas-worker-" + nats.GetClientID(hostname),
		natsURL:   natsURL,

		connMutex:      &sync.RWMutex{},
		maxReconnect:   config.MaxReconnect,
		reconnectDelay: config.ReconnectDelay,
		quitCh:         make(chan struct{}),

		subject:        config.NatsChannel,
		qgroup:         "vanke-faas",
		messageHandler: messageHandler,
		maxInFlight:    config.MaxInflight,
		ackWait:        config.AckWait,
		startSeq:       startSeq,
	}

	started := time.Now()
	if initErr := natsQueue.connect(); initErr != nil {
		log.Panic(initErr)
	}
	for counter <= 0 && time.Since(started).Seconds() <= 60 {
		time.Sleep(50 * time.Millisecond)
	}
	if err := natsQueue.closeConnection(); err != nil {
		log.Panicf("Cannot close connection to %s because of an error: %v\n", natsQueue.natsURL, err)
	}
}

func main() {
	http.HandleFunc("/metrics", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// makeClient constructs a HTTP client with keep-alive turned
// off and a dial-timeout of 30 seconds.
//
// tlsInsecure is required for callbacks to internal services which
// may not have a trusted CA root, or may be misconfigured
func makeClient(tlsInsecure bool) http.Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 0,
		}).DialContext,

		MaxIdleConns:          1,
		DisableKeepAlives:     true,
		IdleConnTimeout:       120 * time.Millisecond,
		ExpectContinueTimeout: 1500 * time.Millisecond,
	}
	if tlsInsecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: tlsInsecure}
	}

	proxyClient := http.Client{
		Transport: tr,
	}

	return proxyClient
}

func postResult(client *http.Client, functionRes *http.Response, result []byte, callbackURL string, xCallID string,
	statusCode int, functionName string, timeTaken float64) (int, error) {
	var reader io.Reader

	if result != nil {
		reader = bytes.NewReader(result)
	}

	request, err := http.NewRequest(http.MethodPost, callbackURL, reader)

	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("unable to post result, error: %s", err.Error())
	}

	if functionRes != nil {
		copyHeaders(request.Header, &functionRes.Header)
	}

	request.Header.Set("X-Duration-Seconds", fmt.Sprintf("%f", timeTaken))
	request.Header.Set("X-Function-Status", fmt.Sprintf("%d", statusCode))
	request.Header.Set("X-Function-Name", functionName)

	if len(xCallID) > 0 {
		request.Header.Set("X-Call-Id", xCallID)
	}

	res, err := client.Do(request)

	if err != nil {
		return http.StatusBadGateway, fmt.Errorf("error posting result to URL %s %s", callbackURL, err.Error())
	}

	if request.Body != nil {
		defer request.Body.Close()
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	return res.StatusCode, nil
}

func copyHeaders(destination http.Header, source *http.Header) {
	for k, v := range *source {
		vClone := make([]string, len(v))
		copy(vClone, v)
		(destination)[k] = vClone
	}
}

func postReport(client *http.Client, function string, statusCode int, timeTaken float64, gatewayAddress string, credentials *auth.BasicAuthCredentials) (int, error) {
	req := AsyncReport{
		FunctionName: function,
		StatusCode:   statusCode,
		TimeTaken:    timeTaken,
	}

	targetPostback := fmt.Sprintf("http://%s/system/async-report", gatewayAddress)
	reqBytes, _ := json.Marshal(req)
	request, err := http.NewRequest(http.MethodPost, targetPostback, bytes.NewReader(reqBytes))

	if os.Getenv("basic_auth") == "true" && credentials != nil {
		request.SetBasicAuth(credentials.User, credentials.Password)
	}

	defer request.Body.Close()

	res, err := client.Do(request)

	if err != nil {
		return http.StatusGatewayTimeout, fmt.Errorf("cannot post report to %s: %s", targetPostback, err)
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	return res.StatusCode, nil
}

func makeFunctionURL(req *queue.Request, config *QueueWorkerConfig, path, queryString string) string {
	qs := ""
	if len(queryString) > 0 {
		qs = fmt.Sprintf("?%s", strings.TrimLeft(queryString, "?"))
	}
	pathVal := "/"
	if len(path) > 0 {
		pathVal = path
	}

	var functionURL string
	if config.GatewayInvoke {
		functionURL = fmt.Sprintf("http://%s/function/%s%s%s",
			config.GatewayAddressURL(),
			strings.Trim(req.Function, "/"),
			pathVal,
			qs)
	} else {
		functionURL = fmt.Sprintf("http://%s%s:8080%s%s",
			req.Function,
			config.FunctionSuffix,
			pathVal,
			qs)
	}

	return functionURL
}
