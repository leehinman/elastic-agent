// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package translate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent/internal/pkg/agent/application/paths"
)

func TestESToOTelConfigPersistentQueue(t *testing.T) {
	top := t.TempDir()
	prev := paths.Top()
	paths.SetTop(top)
	t.Cleanup(func() { paths.SetTop(prev) })

	base := `
hosts: ["localhost:9200"]
username: elastic
password: changeme
`
	t.Run("enabled", func(t *testing.T) {
		cfg := config.MustNewConfigFrom(base + `
queue:
  persistent:
    file_storage:
      timeout: 1s
      max_size: 262144000
      fsync: true
`)
		got, _, ext, err := ESToOTelConfig(cfg, "default", logp.NewNopLogger())
		require.NoError(t, err)

		id := getFileStorageExtensionID("default").String()
		sq := got["sending_queue"].(map[string]any)
		require.Equal(t, id, sq["storage"])
		require.NotContains(t, sq, "wait_for_result")
		require.EqualValues(t, 2147483647, got["retry"].(map[string]any)["max_retries"])

		dir := filepath.Join(paths.Data(), "exporter-queue", "default")
		fs := ext[id].(map[string]any)
		require.Equal(t, dir, fs["directory"])
		require.Equal(t, false, fs["create_directory"])
		require.Equal(t, true, fs["recreate"])
		require.Equal(t, true, fs["fsync"])
		comp := fs["compaction"].(map[string]any)
		require.Equal(t, filepath.Join(dir, "compaction"), comp["directory"])
		require.EqualValues(t, 125, comp["rebound_needed_threshold_mib"])
		require.EqualValues(t, 25, comp["rebound_trigger_threshold_mib"])

		info, err := os.Stat(dir)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	})

	t.Run("disabled", func(t *testing.T) {
		got, _, ext, err := ESToOTelConfig(config.MustNewConfigFrom(base), "default", logp.NewNopLogger())
		require.NoError(t, err)
		sq := got["sending_queue"].(map[string]any)
		require.NotContains(t, sq, "storage")
		require.Equal(t, true, sq["wait_for_result"])
		require.NotContains(t, ext, getFileStorageExtensionID("default").String())
	})
}
