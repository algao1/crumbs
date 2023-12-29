package main

import (
	"crumbs/dbs/lsm"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/patrickmn/go-cache"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

const (
	EXPIRATION_DURATION = 2 * time.Minute
	CLEANUP_INTERVAL    = 15 * time.Second
	DEBUG_MODE          = true
)

type Point struct {
	Metric string
	Tags   map[string]string
	Time   time.Time
	Value  int
}

func metricKey(metric string, tags map[string]string) string {
	key := metric
	tagKeys := maps.Keys[map[string]string](tags)

	slices.Sort[[]string](tagKeys)
	for _, tagKey := range tagKeys {
		key += fmt.Sprintf(",%s=%s", tagKey, tags[tagKey])
	}
	return key
}

func timeKey(ts time.Time, id int) string {
	return fmt.Sprintf("%d@%d", ts.Truncate(time.Minute).Unix(), id)
}

func payloadToBytes(payload []Point) []byte {
	uncompressedVals := make([]byte, len(payload)*8)
	secondsBitMap := make([]byte, 60)

	for i, p := range payload {
		binary.BigEndian.PutUint64(uncompressedVals[i*8:i*8+8], uint64(p.Value))
		secondsBitMap[p.Time.Second()] = 1
	}

	uncompressedVals = append(uncompressedVals, secondsBitMap...)
	return uncompressedVals
}

func bytesToPayload(b []byte, minuteTs time.Time) []Point {
	payload := []Point{}

	uncompressedVals := b[:len(b)-60]
	secondsBitMap := b[len(b)-60:]
	offset := 0

	for second, byt := range secondsBitMap {
		if byt == 0 {
			continue
		}
		payload = append(payload, Point{
			Time:  minuteTs.Add(time.Duration(second) * time.Second),
			Value: int(binary.BigEndian.Uint64(uncompressedVals[offset : offset+8])),
		})
		offset += 8
	}

	return payload
}

type TimeSeriesStore struct {
	curMetricId int
	memStore    *cache.Cache
	metaStore   *lsm.LSMTree
	dataStore   *lsm.LSMTree
	logger      *slog.Logger
}

func NewTimeSeriesStore(dir string) (*TimeSeriesStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("unable to initialize directory: %w", err)
	}
	metaStore, _ := lsm.NewLSMTree(dir + "/meta")
	dataStore, _ := lsm.NewLSMTree(dir + "/data")

	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	if DEBUG_MODE {
		opts.Level = slog.LevelDebug
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)

	s := &TimeSeriesStore{
		memStore:  cache.New(EXPIRATION_DURATION, CLEANUP_INTERVAL),
		metaStore: metaStore,
		dataStore: dataStore,
		logger:    logger,
	}
	s.memStore.OnEvicted(func(k string, v interface{}) {
		s.writePoints(*v.(*[]Point))
		s.logger.Debug(
			"evicted points from mem to disk",
			slog.String("metricId", k),
			slog.Int("points", len(*v.(*[]Point))),
		)
	})
	return s, nil
}

func (s *TimeSeriesStore) Put(p Point) error {
	mKey := metricKey(p.Metric, p.Tags)
	v, err := s.metaStore.Get(mKey)
	if err != nil {
		return err
	}

	var metricId int
	if v == nil {
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, uint64(s.curMetricId))
		metricId = s.curMetricId

		s.metaStore.Put(mKey, idBytes)
		s.curMetricId++
	} else {
		metricId = int(binary.BigEndian.Uint64(v))
	}
	tKey := timeKey(p.Time, metricId)

	pointSlice := &[]Point{}
	s.memStore.Add(timeKey(p.Time, metricId), pointSlice, EXPIRATION_DURATION)

	res, found := s.memStore.Get(tKey)
	if !found {
		return fmt.Errorf("something went wrong, pointSlice not found")
	}
	pointSlice = res.(*[]Point)
	*pointSlice = append(*pointSlice, p)

	return nil
}

func (s *TimeSeriesStore) Get(metric string, tags map[string]string, ts time.Time) ([]Point, error) {
	mKey := metricKey(metric, tags)
	v, err := s.metaStore.Get(mKey)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, fmt.Errorf("metricId not found for %s", mKey)
	}
	metricId := int(binary.BigEndian.Uint64(v))

	res, found := s.memStore.Get(timeKey(ts, metricId))
	if !found {
		points, err := s.readPoints(metricId, ts)
		if err != nil {
			return nil, fmt.Errorf("points not found for %s", mKey)
		}
		return points, nil
	}

	return *res.(*[]Point), nil
}

func (s *TimeSeriesStore) writePoints(points []Point) error {
	mKey := metricKey(points[0].Metric, points[0].Tags)
	v, err := s.metaStore.Get(mKey)
	if err != nil {
		return err
	}
	if v == nil {
		return fmt.Errorf("metricId not found for %s", mKey)
	}
	metricId := int(binary.BigEndian.Uint64(v))

	s.dataStore.Put(timeKey(points[0].Time, metricId), payloadToBytes(points))
	return nil
}

func (s *TimeSeriesStore) readPoints(metricId int, ts time.Time) ([]Point, error) {
	payloadBytes, err := s.dataStore.Get(timeKey(ts, metricId))
	if err != nil {
		return nil, err
	}
	points := bytesToPayload(payloadBytes, ts)
	return points, nil
}

func cleanUp() {
	os.RemoveAll("tss_data")
}

func main() {
	defer cleanUp()
	tss, err := NewTimeSeriesStore("tss_data")
	if err != nil {
		panic(err)
	}

	tss.Put(Point{})
}
