package supervisor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// defaultRetries mirrors the default value of the --retries flag.
const defaultRetries = 5

func fireImmediately(time.Duration) <-chan time.Time {
	c := make(chan time.Time, 1)
	c <- time.Now()
	return c
}

// The supervisor waits out this backoff before it restarts connections that dropped, so it
// bounds how long a tunnel stays unreachable after connectivity has been restored. Without
// a ceiling it reached 5m20s with the default --retries=5.
func TestSupervisorRetryBackoffIsBounded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backoff := newReconnectBackoff(defaultRetries)
	var waited time.Duration
	backoff.Clock.After = func(d time.Duration) <-chan time.Time {
		waited = d
		return fireImmediately(d)
	}

	// Well past maxRetries, to cover the retryForever plateau where the backoff would
	// otherwise sit at its maximum indefinitely.
	for i := 0; i < 20; i++ {
		duration, ok := backoff.GetMaxBackoffDuration(ctx)
		require.True(t, ok, "supervisor must retry forever")
		assert.LessOrEqual(t, duration, maxTunnelRetryDuration,
			"retry %d waits up to %v, longer than the %v ceiling", i, duration, maxTunnelRetryDuration)
		require.True(t, backoff.Backoff(ctx))
		// The timer is jittered separately from the reported maximum, so check it too.
		assert.LessOrEqual(t, waited, maxTunnelRetryDuration,
			"retry %d waited %v, longer than the %v ceiling", i, waited, maxTunnelRetryDuration)
	}
}

// A connection that has come back should not stay saddled with the retry count of an earlier
// disruption, otherwise a brief hiccup inherits the previous maxed-out backoff.
func TestSupervisorBackoffResetsAfterGracePeriod(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	currentTime := time.Now()

	backoff := newReconnectBackoff(defaultRetries)
	backoff.Clock.Now = func() time.Time { return currentTime }
	backoff.Clock.After = fireImmediately

	// Drive the backoff to its plateau, as a prolonged outage would.
	for i := 0; i < 10; i++ {
		backoff.Backoff(ctx)
	}
	require.Equal(t, defaultRetries, backoff.Retries())

	// Connections re-establish; the supervisor starts the grace period.
	gracePeriod := backoff.SetGracePeriod()
	assert.LessOrEqual(t, gracePeriod, maxTunnelRetryDuration,
		"grace period %v outlives the %v ceiling, so the retry count stays inflated", gracePeriod, maxTunnelRetryDuration)

	// After staying up for the grace period, an unrelated disruption starts from scratch.
	currentTime = currentTime.Add(gracePeriod + time.Second)
	require.True(t, backoff.Backoff(ctx))
	assert.Equal(t, 1, backoff.Retries(), "retry count was not reset after a stable grace period")
}
