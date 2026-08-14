// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package domain

import (
	"testing"
	"time"
)

func TestAssignmentRecordAuthorityIntervalIsHalfOpen(t *testing.T) {
	cutover := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	end := cutover.Add(time.Minute)
	record := AssignmentRecord{AuthorityFrom: &cutover, AuthorityUntil: &end}
	if record.Authorizes(cutover.Add(-time.Nanosecond)) || !record.Authorizes(cutover) || !record.Authorizes(end.Add(-time.Nanosecond)) || record.Authorizes(end) {
		t.Fatal("authority interval is not [from, until)")
	}
}
