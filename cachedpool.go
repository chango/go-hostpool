package hostpool

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

type cachedResponse struct {
	standardHostPoolResponse
	started time.Time
	ended   time.Time
}

func (r *cachedResponse) Mark(err error) {
	r.Do(func() {
		r.ended = time.Now()
		doMark(err, r)
	})
}

type cached interface {
	Save(fname string) error
	Load(fname string) error
}

type cachedHostPool struct {
	standardHostPool
	cache         []int64
	responseValue int64
	responseCount int64
	index         int
	clusterLoad   float64
	maxResponse   float64
	maxLifespan   time.Duration
}

// make sure cachedHostPool implements interface
var _ cached = &cachedHostPool{}

func NewCached(hosts []string, fname string, targetLoad float64, maxResponse float64, tickerDuration time.Duration, maxLifespan time.Duration) (HostPool, error) {
	stdHP := New(hosts).(*standardHostPool)
	p := &cachedHostPool{
		standardHostPool: *stdHP,
		cache:            make([]int64, epsilonBuckets),
		index:            0,
		clusterLoad:      0,
		maxResponse:      maxResponse,
		maxLifespan:      maxLifespan,
	}
	err := p.Load(fname)

	// spawn off goroutine to check cluster health
	go p.checkHostHealth()

	go func() {
		durationPerBucket := tickerDuration / epsilonBuckets
		ticker := time.Tick(durationPerBucket)
		for {
			<-ticker
			// rotate the bucket and write
			// and then to disk every cycle
			if p.responseCount == 0 || p.responseValue == 0 {
				continue
			}
			// calculate and store weighting
			p.cache[p.index] = p.responseValue / p.responseCount
			if p.index%10 == 0 {
				err := p.Save(fname)
				if err != nil {
					log.Println("cached hostpool encode error:", err)
				}
			}

			// recalculate the cluster average
			p.clusterLoad = p.getClusterResponseTime() / p.maxResponse

			// simple function to optimize the clusterLoad towards the
			// target load specified. See comment for ShouldPassthru for detail
			// we will set the max rejection at 80% of the traffic
			p.clusterLoad = math.Min(2*p.clusterLoad-targetLoad, 0.80)
			// if the load function is > 1
			// we should enter an exploration phase for the targetload
			// what we want to do here i
			fmt.Printf("Pool debug: Index %d calculated load %f with %d responses\n", p.index, p.clusterLoad, p.responseCount)

			p.index++
			p.index = p.index % epsilonBuckets
			p.cache[p.index] = 0
			p.responseCount = 0
			p.responseValue = 0
		}
	}()

	return p, err
}

func (p *cachedHostPool) checkHostHealth() {
	typ := icmpv4EchoRequest
	xid, xseq := rand.Intn(0xffff), rand.Intn(0xffff)
	wb, _ := (&icmpMessage{
		Type: typ, Code: 0,
		Body: &icmpEcho{
			ID: xid, Seq: xseq,
			Data: bytes.Repeat([]byte("ping!"), 3),
		},
	}).Marshal()
	rb := make([]byte, 20+len(wb))
	ticker := time.Tick(20 * time.Second)

	for {
		<-ticker
		for _, h := range p.hostList {
			c, err := net.Dial("ip4:icmp", strings.Split(h.host, ":")[0])
			if err != nil {
				continue
			}
			c.SetDeadline(time.Now().Add(10 * time.Millisecond))

			if _, err := c.Write(wb); err != nil {
				continue
			}

			if _, err := c.Read(rb); err != nil {
				if err.(net.Error).Timeout() {
					// timedout, mark remote as dead
					p.Lock()
					if !h.dead {
						h.dead = true
						h.retryCount = 0
						h.retryDelay = p.initialRetryDelay
						h.nextRetry = time.Now().Add(h.retryDelay)
					}
					p.Unlock()
				}
				log.Printf("Conn.Read failed for bidder %s: %v", h.host, err)
			}
			c.Close()
		}
	}
}

func (p *cachedHostPool) getClusterResponseTime() float64 {
	var bucketCount float64
	var value float64

	for i := 1; i <= epsilonBuckets; i++ {
		pos := (p.index + i) % epsilonBuckets
		bucketValue := float64(p.cache[pos])

		weight := float64(i) / float64(epsilonBuckets)
		if bucketValue > 0 {
			value += bucketValue * weight
			bucketCount++
		}
	}

	if bucketCount == 0 {
		return 0.0
	}

	return value / bucketCount
}

func ShouldPassthru(h *HostPool) bool {
	// We check if clusterLoad is on target. There are some special cases:
	// 1. 2*clusterLoad - target < 0: we never reject.
	// 2. clusterLoad = target: normal linear operation
	// 3. 2*clusterLoad - target > target: we penelize extra based on the difference
	if passPct, p := rand.Float64(), (*h).(*cachedHostPool); p.clusterLoad > 0 && passPct < p.clusterLoad {
		return false
	}
	return true
}

func (p *cachedHostPool) Get() HostPoolResponse {
	p.Lock()
	defer p.Unlock()
	host := p.getRoundRobin()
	started := time.Now()

	return &cachedResponse{
		standardHostPoolResponse: standardHostPoolResponse{host: host, pool: p},
		started:                  started,
	}
}

func (p *cachedHostPool) markSuccess(resp HostPoolResponse) {
	p.standardHostPool.markSuccess(resp)
	cResp, _ := resp.(*cachedResponse)

	host := cResp.host
	duration := cResp.ended.Sub(cResp.started)

	p.Lock()
	defer p.Unlock()
	_, ok := p.hosts[host]
	if !ok {
		log.Println("host %s not in HostPool %v", host, p.Hosts())
		return
	}
	p.responseCount++
	p.responseValue += int64(duration.Seconds() * 1000) // in milli
}

func (p *cachedHostPool) Save(fname string) (err error) {
	fp, err := os.Create(fname)
	if err != nil {
		return
	}
	defer fp.Close()

	enc := gob.NewEncoder(fp)
	err = enc.Encode(p.cache)
	if err != nil {
		return err
	}

	err = enc.Encode(p.index)
	if err != nil {
		return err
	}

	return enc.Encode(p.nextHostIndex)
}

func (p *cachedHostPool) Load(fname string) (err error) {
	finfo, err := os.Stat(fname)
	if err != nil {
		return
	}
	fileAge := time.Duration(time.Now().Unix()-finfo.ModTime().Unix()) * time.Second
	if fileAge >= p.maxLifespan {
		fmt.Println("Cache file is too old. It's okay, we will just leave cache empty.")
		return
	}

	fp, err := os.Open(fname)
	if err != nil {
		return
	}
	defer fp.Close()

	dec := gob.NewDecoder(fp)
	err = dec.Decode(&p.cache)
	if err != nil {
		return
	}

	err = dec.Decode(&p.index)
	if err != nil {
		return
	}

	return dec.Decode(&p.nextHostIndex)
}
