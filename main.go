package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/cors"
	"github.com/labstack/gommon/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shinomontaz/numbooking/internal/api"
	"github.com/shinomontaz/numbooking/internal/errorer"
)

var env *config.Env

func init() {
	env = config.NewEnv("./config")
	env.InitDb()
	env.InitLog()
	errors = make(chan error, 1000)
}

func main() {
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go errorer.Listen(errors)

	srv := api.New(env.Db, errors)

	mux := NewMux()
	httpsrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", env.Config.ListenPort),
		Handler: mux,
	}
	mux.Post("/reserve", srv.Reserve)
	mux.Get("/numbers", srv.GetNumbers)
	mux.Get("/metrics", promhttp.Handler().ServeHTTP)

	go func() {
		if err := httpsrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	log.Debug("server started on port: ", env.Config.ListenPort)

	<-signals

	log.Debug("server stopped")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		srv.Shutdown(ctx)
		cancel()
	}()

	if err := httpsrv.Shutdown(ctx); err != nil {
		log.Printf("Shutdown error %+v\n", err)

		os.Exit(1)
	}
}

func NewMux() *chi.Mux {
	router := chi.NewRouter()

	cors := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		AllowCredentials: true,
		MaxAge:           300,
	})
	router.Use(
		middleware.Compress(5, "gzip"),
		middleware.RedirectSlashes,
		middleware.Recoverer,
		cors.Handler,
	)
	//	router.Use(middleware.Logger)

	return router
}
