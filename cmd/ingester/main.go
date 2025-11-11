package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/ingester"
	"github.com/censys/scan-takehome/pkg/ingester/cache"
	"github.com/censys/scan-takehome/pkg/ingester/repository"
	"github.com/censys/scan-takehome/pkg/log"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/v2"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"go.uber.org/zap"
)

func main() {
	k := koanf.New(".")
	loadConfigValues(k)
	level := k.String("log.level")
	output := k.String("log.output")
	projectID := k.String("projectid")
	topicID := k.String("topicid")
	workers := k.Int("worker.count")
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	flushInterval := k.Duration("flush.interval")

	maxBatchSize := k.Int("max.batch.size")

	dbURL := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		k.String("postgres.user"),
		k.String("postgres.password"),
		k.String("postgres.host"),
		k.Int("postgres.port"),
		k.String("postgres.db"),
		k.String("postgres.ssl.mode"),
	)

	l, err := log.New(level, output)
	if err != nil {
		println("failed to create logger: ", err.Error())
		os.Exit(1)
	}
	l.Info("starting ingester service...")
	ctx, fn := context.WithCancel(context.Background())

	conn, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		l.Fatal("failed to get postgres connection", zap.Error(err))
	}

	repo := repository.NewPostgresRepository(conn)

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		l.Fatal("failed to create pubsub client", zap.Error(err))
	}

	topic := client.Topic(topicID)

	// if we were deploying this to a k8s cluster we'd consider using something
	// like the podUID or or the IP address of the pod. In this case we'll
	// just use a rand. I'm not sure what docker-compose provides as an env var
	// for unique identifiers.
	subID := fmt.Sprintf("%s-%d", projectID, rand.Int())
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	if err != nil {
		l.Fatal("failed to create pubsub subscription", zap.Error(err))
	}

	// set the number of workers to the number of goroutines that the pubsub
	// subscription will use. In kafka we'd have a single poller and fan out
	// to many workers, but google likes to use callbacks and manage their own
	// goroutines.
	sub.ReceiveSettings.NumGoroutines = workers

	ingest := ingester.NewIngester(l, sub)

	var redisCache *cache.Cache
	if k.Bool("redis.enabled") {
		rcl := redis.NewClient(&redis.Options{
			Addr:     k.String("redis.addr"),
			Password: k.String("redis.password"),
			DB:       k.Int("redis.db"),
			// because I hate the logs that come out of this at startup
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeDisabled,
			},
		})
		if _, err := rcl.Ping(ctx).Result(); err != nil {
			l.Fatal("failed to ping with redis connection,  exiting", zap.Error(err))
		}
		redisCache = cache.NewCache(rcl, k.Duration("redis.ttl"))
	}

	var wg sync.WaitGroup

	l.Info("starting upsert workers", zap.Int("workerCount", workers))
	for range workers {
		up := ingester.NewUpserter(
			l,
			ingest.SendChan,
			repo,
			flushInterval,
			maxBatchSize,
			ingester.WithCache(redisCache),
		)
		wg.Add(1)
		go up.Start(ctx, &wg)
	}

	go ingest.Start(ctx)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	l.Info("shutdown signal received, finishing up....")
	fn()

	shutdownCtx, fn := context.WithTimeout(context.Background(), time.Second*30)
	defer fn()

	if err = sub.Delete(shutdownCtx); err != nil {
		l.Error("failed to delete subscription!", zap.Error(err))
	}

	wg.Wait()

	l.Info("goodbye!")
}

func loadConfigValues(k *koanf.Koanf) {
	prefix := "INGESTER_"
	k.Load(env.Provider(".", env.Opt{
		Prefix: prefix,
		TransformFunc: func(k, v string) (string, any) {
			k = strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(k, prefix)), "_", ".")

			if strings.Contains(v, " ") {
				return k, strings.Split(v, " ")
			}
			return k, v
		},
	}), nil)
}
