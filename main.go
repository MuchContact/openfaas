package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/nats-io/stan.go"
	"github.com/openfaas/faas-provider/auth"
	"github.com/openfaas/faas/gateway/queue"
	"github.com/openfaas/nats-queue-metric/nats"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	//"github.com/openfaas/nats-queue-worker/version"
)

// request /channelsz?sub=1
func requestNatsServer2GetChannelsz(config *QueueWorkerConfig) (uint64, uint64) {
	client := makeClient(config.TLSInsecure)
	natsURL := fmt.Sprintf("http://%s:%d/streaming/channelsz?sub=1", config.NatsAddress, config.NatsPort)
	request, err := http.NewRequest(http.MethodGet, natsURL, nil)
	if err != nil {
		panic("获取channel消费信息失败")

	}
	defer request.Body.Close()
	res, err := client.Do(request)

	var functionResult []byte
	//var statusCode int
	if err != nil {
		//statusCode = http.StatusServiceUnavailable
		if err != nil {
			log.Printf("Error reading body for: %s, error: %s", natsURL, err)
			panic("Error reading body")
		}
	} else {
		//statusCode = res.StatusCode
		resData, err := ioutil.ReadAll(res.Body)
		functionResult = resData
		if err != nil {
			log.Printf("Error reading body for: %s, error: %s", natsURL, err)
			panic("Error reading body")
		}
	}
	body := NSChannelsz{}
	unmarshalErr := json.Unmarshal(functionResult, &body)

	if unmarshalErr != nil {
		log.Printf("[#%d] Unmarshal error: %s with data %s", unmarshalErr, functionResult)
		panic("解析channelsz返回值错误")
	}
	//resp, err := http.Get(natsURL)
	//if err != nil {
	//	log.Printf("[#%d] Unable to post message due to invalid URL, error: %s", i, err.Error())
	//	panic("获取channel消费信息失败")
	//}

	return body.channels[0].subscriptions[0].lastSeq, body.channels[0].lastSeq
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
	readConfig := ReadConfig{}
	config, configErr := readConfig.Read()
	if configErr != nil {
		panic(configErr)
	}
	counter := uint64(0)

	// request channelsz to get current consume status
	startSeq, endSeq := requestNatsServer2GetChannelsz(&config)
	// start a new consumer to status msgs

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
		clientID:  "faas-metric-worker-" + nats.GetClientID(hostname),
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
	for counter <= 0 && int(time.Since(started).Seconds()) <= config.TimeOut {
		time.Sleep(50 * time.Millisecond)
	}
	if err := natsQueue.closeConnection(); err != nil {
		log.Panicf("Cannot close connection to %s because of an error: %v\n", natsQueue.natsURL, err)
	}
	//redisMap := queryRedis(&config)

}
func mergeMap(one map[string]*uint64, two map[string]*uint64) map[string]uint64 {
	result := make(map[string]uint64)

	for k, v := range one {
		result[k] = *v
	}

	for k, v := range two {
		target := result[k]
		if target == 0 {
			result[k] = *v
		} else {
			fmt.Printf("1 %s: %d  %d\n", k, result[k], *v)
			result[k] = result[k] + *v
			fmt.Printf("2 %s: %d  %d\n", k, result[k], *v)
		}
	}

	return result
}

func queryRedis(config *QueueWorkerConfig) map[string]*uint64 {
	metricGroupByFunc := make(map[string]*uint64)
	if !config.RedisEnable {
		return metricGroupByFunc
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPassword, // no password set
		DB:       config.RedisDB,       // use default DB
	})

	keys, _ := rdb.Keys("faas:req:*").Result()
	for _, key := range keys {
		funcName := strings.Split(key, ":")[2]
		target := metricGroupByFunc[funcName]
		if target == nil {
			initCounter := uint64(1)
			metricGroupByFunc[funcName] = &initCounter
		} else {
			atomic.AddUint64(target, 1)
		}
	}
	return metricGroupByFunc
}

func fakehandler(w http.ResponseWriter, r *http.Request) {
	//queryRedis()

	//one := make(map[string]*uint64)
	//it := uint64(3)
	//one["one"] = &it
	//
	//two := make(map[string]*uint64)
	//it2 := uint64(7)
	//two["one"] = &it2
	//it3 := uint64(8)
	//two["two"] = &it3
	//m := mergeMap(one, two)
	//for k,v:=range m{
	//	log.Printf("%s : %d", k, v)
	//}
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
