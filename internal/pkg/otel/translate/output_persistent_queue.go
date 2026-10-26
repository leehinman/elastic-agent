// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package translate

import (
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"

	otelcomponent "go.opentelemetry.io/collector/component"

	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent/internal/pkg/agent/application/paths"
)

const (
	FileStorageExtensionType = "file_storage"

	persistentQueueConfigPath = "queue.persistent.file_storage"
	persistentQueueDirName    = "exporter-queue"
	mib                       = 1 << 20
	persistentQueueMaxRetries = math.MaxInt32
)

func getFileStorageExtensionID(outputName string) otelcomponent.ID {
	return otelcomponent.NewIDWithName(otelcomponent.MustNewType(FileStorageExtensionType), OtelNamePrefix+outputName)
}

func persistentQueueDir(outputName string) string {
	return filepath.Join(paths.Data(), persistentQueueDirName, outputName)
}

// getFileStorageExtensionConfig returns nil if persistent queue is not configured.
func getFileStorageExtensionConfig(output *config.C, outputName string) (map[string]any, error) {
	if !output.HasField("queue") {
		return nil, nil
	}
	fsCfg, err := output.Child(persistentQueueConfigPath, -1)
	if err != nil {
		return nil, nil
	}

	extCfg := map[string]any{}
	if err := fsCfg.Unpack(&extCfg); err != nil {
		return nil, fmt.Errorf("failed unpacking %s: %w", persistentQueueConfigPath, err)
	}

	dir := persistentQueueDir(outputName)
	compactionDir := filepath.Join(dir, "compaction")
	for _, d := range []string{dir, compactionDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("failed creating persistent queue directory %q: %w", d, err)
		}
	}

	compaction, _ := extCfg["compaction"].(map[string]any)
	compaction = maps.Clone(compaction)
	if compaction == nil {
		compaction = map[string]any{}
	}
	compaction["directory"] = compactionDir
	compaction["on_start"] = true
	compaction["on_rebound"] = true
	compaction["cleanup_on_start"] = true
	if maxSize, err := fsCfg.Int("max_size", -1); err == nil && maxSize > 0 {
		compaction["rebound_needed_threshold_mib"] = max(maxSize/2/mib, 1)
		compaction["rebound_trigger_threshold_mib"] = max(maxSize/10/mib, 1)
	}

	extCfg["directory"] = dir
	extCfg["create_directory"] = false
	extCfg["recreate"] = true
	extCfg["compaction"] = compaction
	return extCfg, nil
}

// applyPersistentQueue wires a file_storage extension into exporterCfg's sending_queue
// when queue.persistent is configured. It is a no-op otherwise.
func applyPersistentQueue(output *config.C, outputName string, exporterCfg, extensionCfg map[string]any) error {
	fileStorageCfg, err := getFileStorageExtensionConfig(output, outputName)
	if err != nil || fileStorageCfg == nil {
		return err
	}
	sendingQueue, ok := exporterCfg["sending_queue"].(map[string]any)
	if !ok {
		return fmt.Errorf("exporter has no sending_queue to attach persistent storage to")
	}
	storageID := getFileStorageExtensionID(outputName).String()
	extensionCfg[storageID] = fileStorageCfg
	sendingQueue["storage"] = storageID
	// the exporterhelper rejects wait_for_result together with storage
	delete(sendingQueue, "wait_for_result")
	if retry, ok := exporterCfg["retry"].(map[string]any); ok {
		if _, ok := retry["max_retries"]; ok {
			retry["max_retries"] = persistentQueueMaxRetries
		}
	}
	return nil
}
