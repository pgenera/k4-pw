package pipewire

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.RxName != "k4_rx" || cfg.RxDescription != "Elecraft K4 RX" ||
		cfg.TxName != "k4_tx" || cfg.TxDescription != "Elecraft K4 TX" {
		t.Fatalf("unexpected DefaultConfig: %+v", cfg)
	}
}

func TestParseNodeNames(t *testing.T) {
	sample := `
	id 30, type PipeWire:Interface:Node/3
 		object.serial = "30"
 		factory.id = "11"
 		priority.driver = "200000"
 		node.name = "Dummy-Driver"
	id 44, type PipeWire:Interface:Client/3
 		node.name = "should-ignore-non-node"
	id 56, type PipeWire:Interface:Node/3
 		object.serial = "56"
 		node.description = "Elecraft K4 RX"
 		node.name = "k4_rx"
 		media.class = "Audio/Source/Virtual"
	id 57, type PipeWire:Interface:Node/3
 		object.serial = "57"
 		node.description = "Elecraft K4 TX"
 		node.name="k4_tx"
 		media.class = "Audio/Sink"
	id 58, type PipeWire:Interface:Node/3
 		node.name = "quoted \"name\" test"
`
	got := ParseNodeNames(sample)
	want := []string{
		"Dummy-Driver",
		"k4_rx",
		"k4_tx",
		`quoted "name" test`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseNodeNames() = %#v, want %#v", got, want)
	}
}

func TestStartEphemeralValidation(t *testing.T) {
	_, err := StartEphemeral(context.Background(), Config{
		RxName: "same_node",
		TxName: "same_node",
	})
	if err == nil {
		t.Fatalf("expected error when RxName == TxName")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = StartEphemeral(ctx, Config{
		RxName: "k4_rx_canceled",
		TxName: "k4_tx_canceled",
	})
	if err == nil {
		t.Fatalf("expected error for canceled context")
	}
}

func TestStartEphemeralAndCloseIntegration(t *testing.T) {
	if _, err := exec.LookPath("pw-cli"); err != nil {
		t.Skipf("pw-cli not installed: %v", err)
	}
	if err := exec.Command("pw-cli", "info", "0").Run(); err != nil {
		t.Skipf("PipeWire daemon not reachable: %v", err)
	}

	pid := os.Getpid()
	cfg := Config{
		RxName:        fmt.Sprintf("k4_rx_test_%d", pid),
		RxDescription: fmt.Sprintf("Elecraft K4 RX Test %d", pid),
		TxName:        fmt.Sprintf("k4_tx_test_%d", pid),
		TxDescription: fmt.Sprintf("Elecraft K4 TX Test %d", pid),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	vn, err := StartEphemeral(ctx, cfg)
	if err != nil {
		t.Fatalf("StartEphemeral failed: %v", err)
	}
	defer func() {
		_ = vn.Close()
	}()

	rxOK, txOK, err := CheckNodesExist(cfg.RxName, cfg.TxName)
	if err != nil {
		t.Fatalf("CheckNodesExist failed: %v", err)
	}
	if !rxOK || !txOK {
		t.Fatalf("expected both nodes to exist, got rxOK=%v txOK=%v", rxOK, txOK)
	}

	raw, err := exec.Command("pw-cli", "ls", "Node").Output()
	if err != nil {
		t.Fatalf("pw-cli ls Node failed: %v", err)
	}
	rawStr := string(raw)
	if !strings.Contains(rawStr, cfg.RxDescription) || !strings.Contains(rawStr, cfg.TxDescription) {
		t.Fatalf("expected descriptions %q and %q in pw-cli ls Node output", cfg.RxDescription, cfg.TxDescription)
	}

	if err := vn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	// Verify Close is idempotent.
	if err := vn.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}

	rxOK, txOK, err = CheckNodesExist(cfg.RxName, cfg.TxName)
	if err != nil {
		t.Fatalf("CheckNodesExist after Close failed: %v", err)
	}
	if rxOK || txOK {
		t.Fatalf("expected ephemeral nodes to vanish after Close, got rxOK=%v txOK=%v", rxOK, txOK)
	}
}
