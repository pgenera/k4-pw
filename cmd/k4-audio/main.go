package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k4-audio/internal/audio"
	"k4-audio/internal/bridge"
	"k4-audio/internal/k4proto"
	"k4-audio/internal/pipewire"
)

type cliConfig struct {
	Host             string
	Port             int
	Password         string
	PasswordFile     string
	RxGainDb         float64
	TxGainDb         float64
	BufferMs         int
	SilenceThreshold int
	TxHoldMs         int
	TxCatOnAudio     bool
	Backend          string
	RxNode           string
	TxNode           string
	NoEphemeralNodes bool
	Once             bool
	LogLevels        bool
}

func defaultCLIConfig() cliConfig {
	pwDef := pipewire.DefaultConfig()
	return cliConfig{
		Port:             9205,
		BufferMs:         40,
		SilenceThreshold: 2,
		TxHoldMs:         100,
		Backend:          "auto",
		RxNode:           pwDef.RxName,
		TxNode:           pwDef.TxName,
	}
}

func defaultConfigPath() (string, error) {
	cfgDir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cfgDir, "k4-audio", "config.env"), nil
}

func loadRuntimeConfig() (cliConfig, error) {
	cfg := defaultCLIConfig()
	if cfgPath, err := defaultConfigPath(); err == nil {
		if err := loadConfigEnv(cfgPath, &cfg); err != nil {
			return cfg, err
		}
	}
	if err := applyProcessEnv(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func loadConfigEnv(path string, cfg *cliConfig) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		key, val, ok, err := parseEnvLine(scanner.Text())
		if err != nil {
			return fmt.Errorf("%s:%d: %w", path, lineNo, err)
		}
		if !ok {
			continue
		}
		if err := applyConfigKey(key, val, cfg, true); err != nil {
			return fmt.Errorf("%s:%d (%s): %w", path, lineNo, key, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", path, err)
	}
	return nil
}

func parseEnvLine(line string) (key, val string, ok bool, err error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return "", "", false, nil
	}
	if strings.HasPrefix(trimmed, "export") && len(trimmed) > 6 && (trimmed[6] == ' ' || trimmed[6] == '\t') {
		trimmed = strings.TrimSpace(trimmed[6:])
	}
	eq := strings.IndexByte(trimmed, '=')
	if eq <= 0 {
		return "", "", false, fmt.Errorf("expected KEY=VALUE")
	}
	key = strings.TrimSpace(trimmed[:eq])
	val = strings.TrimSpace(trimmed[eq+1:])
	if len(val) >= 2 {
		if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
			val = val[1 : len(val)-1]
		}
	}
	return key, val, true, nil
}

var envKeys = []string{
	"K4_HOST",
	"K4_PORT",
	"K4_PASSWORD",
	"K4_PASSWORD_FILE",
	"K4_RX_GAIN_DB",
	"K4_TX_GAIN_DB",
	"K4_BUFFER_MS",
	"K4_SILENCE_THRESHOLD",
	"K4_TX_HOLD_MS",
	"K4_TX_CAT_ON_AUDIO",
	"K4_BACKEND",
	"K4_RX_NODE",
	"K4_TX_NODE",
	"K4_NO_EPHEMERAL_NODES",
	"K4_ONCE",
	"K4_LOG_LEVELS",
}

func applyProcessEnv(cfg *cliConfig) error {
	for _, k := range envKeys {
		if v, ok := os.LookupEnv(k); ok {
			if err := applyConfigKey(k, v, cfg, false); err != nil {
				return fmt.Errorf("env %s: %w", k, err)
			}
		}
	}
	return nil
}

func applyConfigKey(key, val string, cfg *cliConfig, allowUnprefixed bool) error {
	norm := strings.ToUpper(strings.TrimSpace(key))
	if allowUnprefixed && !strings.HasPrefix(norm, "K4_") {
		norm = "K4_" + norm
	}

	switch norm {
	case "K4_HOST":
		cfg.Host = val
	case "K4_PORT":
		n, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		cfg.Port = n
	case "K4_PASSWORD":
		cfg.Password = val
	case "K4_PASSWORD_FILE":
		cfg.PasswordFile = val
	case "K4_RX_GAIN_DB":
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return err
		}
		cfg.RxGainDb = f
	case "K4_TX_GAIN_DB":
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return err
		}
		cfg.TxGainDb = f
	case "K4_BUFFER_MS":
		n, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		cfg.BufferMs = n
	case "K4_SILENCE_THRESHOLD":
		n, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		cfg.SilenceThreshold = n
	case "K4_TX_HOLD_MS":
		n, err := strconv.Atoi(val)
		if err != nil {
			return err
		}
		cfg.TxHoldMs = n
	case "K4_TX_CAT_ON_AUDIO":
		b, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		cfg.TxCatOnAudio = b
	case "K4_BACKEND":
		cfg.Backend = val
	case "K4_RX_NODE":
		cfg.RxNode = val
	case "K4_TX_NODE":
		cfg.TxNode = val
	case "K4_NO_EPHEMERAL_NODES":
		b, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		cfg.NoEphemeralNodes = b
	case "K4_ONCE":
		b, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		cfg.Once = b
	case "K4_LOG_LEVELS":
		b, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		cfg.LogLevels = b
	}
	return nil
}

func expandHomePath(p string) (string, error) {
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		if p == "~" {
			return home, nil
		}
		return filepath.Join(home, p[2:]), nil
	}
	return p, nil
}

func resolvePassword(cfg cliConfig, pwOnCLI, pwFileOnCLI bool) (string, error) {
	if pwOnCLI {
		return cfg.Password, nil
	}
	if !pwFileOnCLI && cfg.Password != "" {
		return cfg.Password, nil
	}
	if cfg.PasswordFile == "" {
		return "", nil
	}
	path, err := expandHomePath(cfg.PasswordFile)
	if err != nil {
		return "", err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read password file %s: %w", path, err)
	}
	return strings.TrimRight(string(raw), "\r\n"), nil
}

func validateConfig(cfg cliConfig) error {
	if strings.TrimSpace(cfg.Host) == "" {
		return errors.New("missing required --host (or K4_HOST in ~/.config/k4-audio/config.env)")
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return fmt.Errorf("invalid --port %d (must be 1..65535)", cfg.Port)
	}
	if cfg.BufferMs <= 0 {
		return fmt.Errorf("invalid --buffer-ms %d (must be > 0)", cfg.BufferMs)
	}
	if cfg.SilenceThreshold < 0 || cfg.SilenceThreshold > 32767 {
		return fmt.Errorf("invalid --silence-threshold %d (must be 0..32767)", cfg.SilenceThreshold)
	}
	if cfg.TxHoldMs < 0 {
		return fmt.Errorf("invalid --tx-hold-ms %d (must be >= 0)", cfg.TxHoldMs)
	}
	if cfg.RxNode == "" || cfg.TxNode == "" {
		return errors.New("--rx-node and --tx-node must be non-empty")
	}
	if cfg.RxNode == cfg.TxNode {
		return fmt.Errorf("--rx-node and --tx-node must differ (%q)", cfg.RxNode)
	}
	return nil
}

func printWSJTXSummary(w io.Writer, host, rxNode, txNode string) {
	pwDef := pipewire.DefaultConfig()
	targetHost := strings.TrimSpace(host)
	if targetHost == "" {
		targetHost = "<k4-ip>"
	}
	fmt.Fprintln(w, "wsjtx-improved configuration:")
	fmt.Fprintf(w, "  Radio: Rig=Elecraft K4, Network Server=%s:9200, PTT=CAT, Mode=Data/Pkt\n", targetHost)
	fmt.Fprintf(w, "  Audio Input:  %s (node %s) -> Left (Main RX) or Right (Sub RX)\n", pwDef.RxDescription, rxNode)
	fmt.Fprintf(w, "  Audio Output: %s (node %s)\n", pwDef.TxDescription, txNode)
}

func runCheck(args []string) int {
	cfg, err := loadRuntimeConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	fs := flag.NewFlagSet("k4-audio check", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.RxNode, "rx-node", cfg.RxNode, "PipeWire RX source node name")
	fs.StringVar(&cfg.TxNode, "tx-node", cfg.TxNode, "PipeWire TX sink node name")
	fs.StringVar(&cfg.Host, "host", cfg.Host, "Elecraft K4 IP/hostname for configuration summary")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: k4-audio check [--rx-node=k4_rx] [--tx-node=k4_tx] [--host=<k4-ip>]")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "error: unexpected arguments: %s\n", strings.Join(fs.Args(), " "))
		return 2
	}

	rxOK, txOK, err := pipewire.CheckNodesExist(cfg.RxNode, cfg.TxNode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "PipeWire check failed: %v\n", err)
		return 1
	}

	statusLabel := func(ok bool) string {
		if ok {
			return "READY"
		}
		return "MISSING"
	}

	fmt.Printf("PipeWire nodes:\n")
	fmt.Printf("  RX (%s): %s\n", cfg.RxNode, statusLabel(rxOK))
	fmt.Printf("  TX (%s): %s\n", cfg.TxNode, statusLabel(txOK))
	printWSJTXSummary(os.Stdout, cfg.Host, cfg.RxNode, cfg.TxNode)

	if !rxOK || !txOK {
		return 1
	}
	return 0
}

func runBridge(ctx context.Context, cfg cliConfig, logger *log.Logger) error {
	pwDef := pipewire.DefaultConfig()
	if !cfg.NoEphemeralNodes {
		nodes, err := pipewire.StartEphemeral(ctx, pipewire.Config{
			RxName:        cfg.RxNode,
			RxDescription: pwDef.RxDescription,
			TxName:        cfg.TxNode,
			TxDescription: pwDef.TxDescription,
		})
		if err != nil {
			return fmt.Errorf("start pipewire nodes: %w", err)
		}
		logger.Printf("pipewire virtual nodes ready: rx=%s (%q) tx=%s (%q)",
			cfg.RxNode, pwDef.RxDescription, cfg.TxNode, pwDef.TxDescription)
		defer func() {
			if cerr := nodes.Close(); cerr != nil {
				logger.Printf("pipewire teardown error: %v", cerr)
			}
		}()
	}

	eng, err := audio.Open(ctx, audio.Config{
		RxTarget:       cfg.RxNode,
		TxTarget:       cfg.TxNode,
		SampleRate:     k4proto.SampleRate12k,
		FrameSamples:   k4proto.FrameSamples20ms,
		TargetBufferMs: cfg.BufferMs,
		Backend:        cfg.Backend,
	})
	if err != nil {
		return fmt.Errorf("open audio engine: %w", err)
	}
	defer func() {
		if cerr := eng.Close(); cerr != nil {
			logger.Printf("audio engine close error: %v", cerr)
		}
	}()

	st := eng.Stats()
	logger.Printf("audio engine ready: rx_backend=%s tx_backend=%s buffer_ms=%d",
		st.RxBackend, st.TxBackend, cfg.BufferMs)

	sessCfg := bridge.Config{
		Host:             cfg.Host,
		Port:             cfg.Port,
		Password:         cfg.Password,
		RxGainDb:         cfg.RxGainDb,
		TxGainDb:         cfg.TxGainDb,
		SilenceThreshold: int16(cfg.SilenceThreshold),
		TxHoldTail:       time.Duration(cfg.TxHoldMs) * time.Millisecond,
		TxCatOnAudio:     cfg.TxCatOnAudio,
		Reconnect:        !cfg.Once,
		LogLevels:        cfg.LogLevels,
		DialTimeout:      5 * time.Second,
	}

	sess := bridge.NewSession(sessCfg, eng, logger)
	err = sess.Run(ctx)
	if err != nil && errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}

func runMain(args []string) int {
	if len(args) > 0 && args[0] == "check" {
		return runCheck(args[1:])
	}

	cfg, err := loadRuntimeConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	fs := flag.NewFlagSet("k4-audio", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.Host, "host", cfg.Host, "Elecraft K4 IP address or hostname")
	fs.IntVar(&cfg.Port, "port", cfg.Port, "Elecraft K4 remote audio/CAT port")
	fs.StringVar(&cfg.Password, "password", cfg.Password, "Elecraft K4 network password")
	fs.StringVar(&cfg.PasswordFile, "password-file", cfg.PasswordFile, "Path to file containing Elecraft K4 password")
	fs.Float64Var(&cfg.RxGainDb, "rx-gain-db", cfg.RxGainDb, "RX audio gain trim in dB")
	fs.Float64Var(&cfg.TxGainDb, "tx-gain-db", cfg.TxGainDb, "TX audio gain trim in dB")
	fs.IntVar(&cfg.BufferMs, "buffer-ms", cfg.BufferMs, "RX jitter buffer prefill target in milliseconds")
	fs.IntVar(&cfg.SilenceThreshold, "silence-threshold", cfg.SilenceThreshold, "TX audio activity peak threshold (0..32767)")
	fs.IntVar(&cfg.TxHoldMs, "tx-hold-ms", cfg.TxHoldMs, "TX audio gate hold tail in milliseconds")
	fs.BoolVar(&cfg.TxCatOnAudio, "tx-cat-on-audio", cfg.TxCatOnAudio, "Send TX;/RX; CAT commands on audio activity")
	fs.StringVar(&cfg.Backend, "backend", cfg.Backend, "Audio backend: auto, pulse, pw-cat, or mock")
	fs.StringVar(&cfg.RxNode, "rx-node", cfg.RxNode, "PipeWire RX virtual source node name")
	fs.StringVar(&cfg.TxNode, "tx-node", cfg.TxNode, "PipeWire TX virtual sink node name")
	fs.BoolVar(&cfg.NoEphemeralNodes, "no-ephemeral-nodes", cfg.NoEphemeralNodes, "Do not create/destroy ephemeral PipeWire nodes")
	fs.BoolVar(&cfg.Once, "once", cfg.Once, "Exit after first disconnect instead of reconnecting")
	fs.BoolVar(&cfg.LogLevels, "log-levels", cfg.LogLevels, "Log periodic RX/TX audio levels and buffer stats")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "  k4-audio [flags]")
		fmt.Fprintln(os.Stderr, "  k4-audio check [--rx-node=k4_rx] [--tx-node=k4_tx] [--host=<k4-ip>]")
		fmt.Fprintln(os.Stderr, "\nFlags (override ~/.config/k4-audio/config.env):")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "error: unexpected arguments: %s\n", strings.Join(fs.Args(), " "))
		return 2
	}

	var pwOnCLI, pwFileOnCLI bool
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "password":
			pwOnCLI = true
		case "password-file":
			pwFileOnCLI = true
		}
	})

	password, err := resolvePassword(cfg, pwOnCLI, pwFileOnCLI)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}
	cfg.Password = password

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 2
	}

	logger := log.New(os.Stderr, "k4-audio: ", log.LstdFlags)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := runBridge(ctx, cfg, logger); err != nil {
		logger.Printf("fatal: %v", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(runMain(os.Args[1:]))
}
