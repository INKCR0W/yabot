package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/app"
	"github.com/INKCR0W/yabot/internal/platform/config"
	"github.com/INKCR0W/yabot/internal/platform/obs"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	flags := flag.NewFlagSet("yabot", flag.ContinueOnError)
	configPath := flags.String("config", "config.toml", "path to the TOML configuration file")
	showVersion := flags.Bool("version", false, "print the version and exit")
	// Argument values are never echoed in case a secret was passed by mistake.
	flags.SetOutput(io.Discard)
	err := flags.Parse(os.Args[1:])
	if err == nil && flags.NArg() > 0 {
		err = errors.New("unexpected positional arguments")
	}
	if err != nil {
		flags.SetOutput(os.Stderr)
		if errors.Is(err, flag.ErrHelp) {
			flags.Usage()
			return 0
		}
		fmt.Fprintln(os.Stderr, "invalid command line arguments")
		flags.Usage()
		return 2
	}
	if *showVersion {
		if _, err := fmt.Println(version); err != nil {
			slog.Error("print version", "error", err)
			return 1
		}
		return 0
	}

	cfg, err := config.Load(*configPath, os.LookupEnv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		return 1
	}
	// The runtime treats an empty GOMEMLIMIT as unset.
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(cfg.MemoryLimitBytes())
	}
	logger, err := obs.NewLogger(os.Stderr, cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		slog.Error("create logger", "error", err)
		return 1
	}

	set := metrics.NewSet()
	health := obs.NewHealth()
	runner := app.NewRunner(logger)
	fatal := runner.Fatal()
	skew := telegram.NewTimeSkew(logger, set)
	client, err := telegram.New(telegram.Config{
		Token:     cfg.Telegram.Token.Reveal(),
		ServerURL: cfg.Telegram.APIServer,
		Skew:      skew,
		Log:       logger,
	})
	if err != nil {
		logger.Error("create Telegram client", "error", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("starting YaBot",
		"version", version, "go_version", runtime.Version(), "gomemlimit_bytes", debug.SetMemoryLimit(-1),
		"config_path", *configPath, "admin_listen", cfg.Admin.Listen)
	logger.Warn("Run only one instance per Telegram bot token. A second instance can cause getUpdates HTTP 409 conflicts and lost updates.")
	if ok, interrupted := checkTelegram(ctx, client, skew, logger); !ok {
		if interrupted {
			return 0
		}
		return 1
	}

	admin := obs.NewAdmin(obs.AdminDeps{Listen: cfg.Admin.Listen, Metrics: set, Health: health, Log: logger, Fatal: fatal})
	if err := runner.Run(ctx, admin); err != nil {
		logger.Error("YaBot stopped with an error", "error", err)
		return 1
	}
	return 0
}

func checkTelegram(parent context.Context, client *telegram.Client, skew *telegram.TimeSkew, logger *slog.Logger) (ok, interrupted bool) {
	const timeout = 10 * time.Second
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	me, err := client.GetMe(ctx)
	if err != nil {
		outcome := telegram.Classify(err)
		if parent.Err() != nil {
			logger.Info("YaBot interrupted during startup", "class", outcome.Class.String(), "error", err)
			return false, true
		}
		message := "Telegram getMe failed"
		switch {
		case errors.Is(err, bot.ErrorUnauthorized):
			message = "Telegram getMe failed: invalid bot token (401 Unauthorized)"
		case errors.Is(err, bot.ErrorNotFound):
			message = "Telegram getMe failed: malformed bot token or incorrect telegram.api_server (404 Not Found)"
		}
		logger.ErrorContext(ctx, message, "class", outcome.Class.String(), "error", err)
		return false, false
	}

	logger.InfoContext(ctx, "Telegram getMe succeeded", "bot_id", me.ID, "username", me.Username,
		"supports_join_request_queries", me.SupportsJoinRequestQueries, "skew_seconds", skew.Skew().Seconds())
	if !me.SupportsJoinRequestQueries {
		logger.WarnContext(ctx, "Guard requires Telegram Bot API 10.1 join request queries. Enable Guard Mode in BotFather.")
	}
	return true, false
}
