// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package postgres

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func (s *Store) migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended('aero-arc-conformance-migrations',0))`); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtextextended('aero-arc-conformance-migrations',0))`)
	if _, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version bigint PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("initialize migration ledger: %w", err)
	}
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	seenVersions := map[int64]string{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return fmt.Errorf("migration %q has no numeric prefix", entry.Name())
		}
		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil {
			return fmt.Errorf("parse migration %q: %w", entry.Name(), err)
		}
		contents, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		if previous, exists := seenVersions[version]; exists {
			return fmt.Errorf("migration %q duplicates version %d from %q", entry.Name(), version, previous)
		}
		seenVersions[version] = entry.Name()
		checksumBytes := sha256.Sum256(contents)
		checksum := hex.EncodeToString(checksumBytes[:])
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %q: %w", entry.Name(), err)
		}
		var appliedChecksum string
		err = tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, version).Scan(&appliedChecksum)
		applied := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("check migration %q: %w", entry.Name(), err)
		}
		if applied && appliedChecksum != checksum {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %q checksum changed after application", entry.Name())
		}
		if !applied {
			if _, err = tx.Exec(ctx, string(contents)); err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("apply migration %q: %w", entry.Name(), err)
			}
			if _, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)`, version, checksum); err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("record migration %q: %w", entry.Name(), err)
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %q: %w", entry.Name(), err)
		}
	}
	return nil
}
