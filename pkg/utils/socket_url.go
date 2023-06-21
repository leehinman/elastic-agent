// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package utils

import (
	"crypto/sha256"
	"encoding/base64"
	"net/url"
)

const (
	SocketFallbackDirectory   = "/tmp/elastic-agent"
	WindowsNamedPipeMaxLength = 256
	WindowsSocketScheme       = "npipe"
	UnixSocketMaxLength       = 104
	UnixSocketScheme          = "unix"
)

// SocketURLWithFallback builds a path for a unix socket or Windows named pipe.
// There are several restrictions on these paths.
// 1. unix socket paths must be less than 104 characters
// 2. Windows named pipes must be less than 256 characters.
// 3. Windows named pipes are just a filename not a path.
// The ids can often be over these limits.  So we follow this
// algorithm to get unique paths that are less than 104
// characters.
// 1. take sha256 of id
// 2. base64 encode the first 24 bytes of hash (full 32 can be too long)
// 3. use URLencoding for base64, this is filename safe and is shorter than hex
// 4. if this is still to long for unix, use the system temp directory
func SocketURLWithFallback(id, operatingSystem, dir string) string {
	hashID := sha256.Sum256([]byte(id))
	filename := base64.URLEncoding.EncodeToString(hashID[:24]) + ".sock"
	u := &url.URL{}
	u.Path = "/"
	if operatingSystem == "windows" {
		u.Scheme = WindowsSocketScheme
		dir = "/"
	} else {
		u.Scheme = UnixSocketScheme
	}

	candidateURL := u.JoinPath(dir, filename)
	if (operatingSystem == "windows") || (len(candidateURL.String()) < UnixSocketMaxLength) {
		return candidateURL.String()
	}

	candidateURL = u.JoinPath(SocketFallbackDirectory, filename)
	return candidateURL.String()
}
