package queuer

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"golang.org/x/net/http2"
)

type SQSQueue struct {
	svc       *sqs.SQS
	queue     *sqs.GetQueueUrlOutput
	ch        chan Object
	quit      chan bool
	done      chan bool
	started   bool
	pushOnly  bool
	batch     [][]byte
	batchLock *sync.Mutex
}

type sqsMessage struct {
	body []byte
	msg  *sqs.Message
	q    *SQSQueue
}

func (m sqsMessage) Body() []byte {
	return m.body
}

func (m sqsMessage) Done() error {
	_, err := m.q.svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      m.q.queue.QueueUrl,
		ReceiptHandle: m.msg.ReceiptHandle,
	})

	if err != nil {
		return errors.Wrap(err, "couldn't delete message")
	}
	return nil
}

func NewSQS(akey, skey, name, region, endpoint, env string, pushOnly bool) (q *SQSQueue, err error) {
	q = &SQSQueue{}
	var cr *credentials.Credentials
	q.done = make(chan bool)
	q.quit = make(chan bool)
	q.pushOnly = pushOnly
	if !pushOnly {
		q.ch = make(chan Object, 100)
	}
	if env == "production" {
		p := &ec2rolecreds.EC2RoleProvider{
			// Pass in a custom timeout to be used when requesting
			// IAM EC2 Role credentials.
			Client: ec2metadata.New(session.New(), &aws.Config{
				HTTPClient: &http.Client{Timeout: 10 * time.Second},
			}),

			// Do not use early expiry of credentials. If a non zero value is
			// specified the credentials will be expired early
			ExpiryWindow: 0,
		}
		cr = credentials.NewCredentials(p)
		_, err = cr.Get()
		if err != nil {
			return nil, errors.Wrap(err, "couldn't get credentials from EC2")
		}
	} else {
		cr = credentials.NewStaticCredentials(akey, skey, "")
	}

	q.svc = sqs.New(session.New(), &aws.Config{
		Region:      aws.String(region),
		Endpoint:    aws.String(endpoint),
		Credentials: cr,
		// HTTPClient: NewHTTPClientWithSettings(HTTPClientSettings{
		// 	Connect:          5 * time.Second,
		// 	ExpectContinue:   1 * time.Second,
		// 	IdleConn:         90 * time.Second,
		// 	ConnKeepAlive:    30 * time.Second,
		// 	MaxAllIdleConns:  100,
		// 	MaxHostIdleConns: 10,
		// 	ResponseHeader:   5 * time.Second,
		// 	TLSHandshake:     5 * time.Second,
		// }),
	})

	q.queue, err = q.svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		if env != "production" {
			aerr, ok := err.(awserr.Error)
			if ok && (aerr.Code() == sqs.ErrCodeQueueDoesNotExist) {
				q.queue, err = q.createQueue(name)
				if err != nil {
					return nil, errors.Wrap(err, "couldn't create queue")

				}
			} else {
				return nil, errors.Wrap(err, "couldn't get queue")
			}
		} else {
			return nil, errors.Wrap(err, "couldn't get queue")
		}
	}
	return
}

func (q *SQSQueue) EnableBatch() {
	if q.batch != nil {
		return
	}
	q.batch = [][]byte{}
	q.batchLock = &sync.Mutex{}
	go func() {
		for {
			select {
			case <-q.done:
				q.processBatch(true)
				return
			case <-time.After(10 * time.Second):
				q.processBatch(true)
			}
		}
	}()
}

func (q *SQSQueue) Start() chan Object {
	if q.started || q.pushOnly {
		return q.ch
	}
	go func() {
		for {
			select {
			case <-q.quit:
				close(q.done)
				return
			default:
				msgs, _ := q.processQueue()
				if len(msgs) > 0 {
					for _, msg := range msgs {
						m := sqsMessage{
							body: []byte(*msg.Body),
							msg:  msg,
							q:    q,
						}
						q.ch <- m
					}
				}
			}
		}
	}()
	q.started = true
	return q.ch
}

func (q *SQSQueue) Receive() <-chan Object {
	return q.ch
}

func (q *SQSQueue) Stop() {
	close(q.quit)
	<-q.done
	return
}

func (q *SQSQueue) Publish(bts []byte) error {
	_, err := q.svc.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(bts)),
		QueueUrl:    q.queue.QueueUrl,
	})

	if err != nil {
		return errors.Wrap(err, "Couldn't publish SQS message")
	}

	return nil
}

func (q *SQSQueue) Enqueue(bts []byte) {
	q.batchLock.Lock()
	q.batch = append(q.batch, bts)
	q.batchLock.Unlock()
}

func (q *SQSQueue) PublishWithRoutingKey(rkey string, bts []byte) error {
	_, err := q.svc.SendMessage(&sqs.SendMessageInput{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"routing_key": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(rkey),
			},
		},
		MessageBody: aws.String(string(bts)),
		QueueUrl:    q.queue.QueueUrl,
	})

	if err != nil {
		return errors.Wrap(err, "Couldn't publish SQS message")
	}

	return nil
}

func (q *SQSQueue) processQueue() ([]*sqs.Message, error) {
	if q.pushOnly {
		return nil, ErrQueuePushOnly
	}
	var msgs *sqs.ReceiveMessageOutput
	msgs, err := q.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            q.queue.QueueUrl,
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
	})

	if err != nil {
		return nil, errors.Wrap(err, "couldn't receive message")
	}

	return msgs.Messages, nil
}

var batchSize = 10

func (q *SQSQueue) processBatch(force bool) error {
	q.batchLock.Lock()
	if len(q.batch) == 0 {
		q.batchLock.Unlock()
		return nil
	}
	if len(q.batch) >= batchSize || force {
		toSend := make([][]byte, len(q.batch))
		copy(q.batch, toSend)
		q.batch = [][]byte{}
		q.batchLock.Unlock()
		entries := []*sqs.SendMessageBatchRequestEntry{}
		for _, m := range toSend {
			entries = append(entries, &sqs.SendMessageBatchRequestEntry{
				MessageBody: aws.String(string(m)),
			})
		}
		_, err := q.svc.SendMessageBatch(&sqs.SendMessageBatchInput{
			Entries:  entries,
			QueueUrl: q.queue.QueueUrl,
		})
		if err != nil {
			return errors.Wrap(err, "Couldn't publish SQS messages batch")
		}
	}
	return nil
}

func (q *SQSQueue) createQueue(name string) (queue *sqs.GetQueueUrlOutput, err error) {
	result, err := q.svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]*string{
			"DelaySeconds":                  aws.String("0"),
			"ReceiveMessageWaitTimeSeconds": aws.String("20"),
			"MessageRetentionPeriod":        aws.String("86400"),
			"VisibilityTimeout":             aws.String("360"),
		},
	})
	if err != nil {
		err = errors.Wrap(err, "couldn't create queue")
		return
	}
	queue = &sqs.GetQueueUrlOutput{
		QueueUrl: result.QueueUrl,
	}
	return
}

type HTTPClientSettings struct {
	Connect          time.Duration
	ConnKeepAlive    time.Duration
	ExpectContinue   time.Duration
	IdleConn         time.Duration
	MaxAllIdleConns  int
	MaxHostIdleConns int
	ResponseHeader   time.Duration
	TLSHandshake     time.Duration
}

func NewHTTPClientWithSettings(httpSettings HTTPClientSettings) *http.Client {
	tr := &http.Transport{
		ResponseHeaderTimeout: httpSettings.ResponseHeader,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: httpSettings.ConnKeepAlive,
			DualStack: true,
			Timeout:   httpSettings.Connect,
		}).DialContext,
		MaxIdleConns:          httpSettings.MaxAllIdleConns,
		IdleConnTimeout:       httpSettings.IdleConn,
		TLSHandshakeTimeout:   httpSettings.TLSHandshake,
		MaxIdleConnsPerHost:   httpSettings.MaxHostIdleConns,
		ExpectContinueTimeout: httpSettings.ExpectContinue,
	}

	// So client makes HTTP/2 requests
	http2.ConfigureTransport(tr)

	return &http.Client{
		Transport: tr,
	}
}
