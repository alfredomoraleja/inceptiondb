package bootstrap

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/fulldump/box"

	"github.com/fulldump/inceptiondb/api"
	"github.com/fulldump/inceptiondb/configuration"
	"github.com/fulldump/inceptiondb/database"
	"github.com/fulldump/inceptiondb/mongo"
	"github.com/fulldump/inceptiondb/service"
)

var VERSION = "dev"

func Bootstrap(c *configuration.Configuration) (start, stop func()) {

	db := database.NewDatabase(&database.Config{
		Dir: c.Dir,
	})

	srv := service.NewService(db)

	b := api.Build(srv, c.Statics, VERSION)
	if c.EnableCompression {
		b.WithInterceptors(api.Compression)
	}
	b.WithInterceptors(
		api.AccessLog(log.New(os.Stdout, "ACCESS: ", log.Lshortfile)),
		api.InterceptorUnavailable(db),
		api.RecoverFromPanic,
		api.PrettyErrorInterceptor,
	)

	s := &http.Server{
		Addr:    c.HttpAddr,
		Handler: box.Box2Http(b),
	}

	if c.HttpsSelfsigned {
		log.Println("HTTPS Selfsigned")
		s.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{selfSignedCertificate()},
		}
	}

	ln, err := net.Listen("tcp", c.HttpAddr)
	if err != nil {
		log.Println("ERROR:", err.Error())
		os.Exit(-1)
	}
	log.Println("listening on", c.HttpAddr)

	var mongoServer *mongo.Server
	var mongoListener net.Listener
	if c.MongoAddr != "" {
		mongoServer = mongo.NewServer(srv)
		mongoListener, err = net.Listen("tcp", c.MongoAddr)
		if err != nil {
			log.Println("ERROR:", err.Error())
			os.Exit(-1)
		}
		log.Println("mongo wire listening on", c.MongoAddr)
	}

	stop = func() {
		db.Stop()
		s.Shutdown(context.Background())
		if mongoServer != nil {
			mongoServer.Close()
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for {
			sig := <-signalChan
			fmt.Println("Signal received", sig.String())
			stop()
		}
	}()

	start = func() {

		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := db.Start()
			if err != nil {
				fmt.Println(err.Error())
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if c.HttpsEnabled {
				err = s.ServeTLS(ln, "", "")
			} else {
				err = s.Serve(ln)
			}
			if err != nil {
				fmt.Println(err.Error())
			}
		}()

		if mongoServer != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := mongoServer.Serve(mongoListener); err != nil && !errors.Is(err, net.ErrClosed) {
					fmt.Println(err.Error())
				}
			}()
		}

		wg.Wait()
	}

	return
}
