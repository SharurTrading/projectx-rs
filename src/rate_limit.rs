// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Provider REST rate-limit configuration and shared admission control.

use std::{collections::VecDeque, fmt, time::Duration};

use parking_lot::Mutex;
use tokio::{
    sync::Notify,
    time::{Instant, sleep},
};

use crate::{Error, error::ConfigurationError};

const DEFAULT_HISTORY_REQUESTS: usize = 50;
const DEFAULT_HISTORY_WINDOW: Duration = Duration::from_secs(30);
const DEFAULT_GENERAL_REQUESTS: usize = 200;
const DEFAULT_GENERAL_WINDOW: Duration = Duration::from_mins(1);
const MAX_INITIAL_CAPACITY: usize = 1_024;
/// Shared ceiling for one provider cooldown.
///
/// Provider `Retry-After` values are unbounded remote input. The limiter
/// clamps them to this ceiling so no caller-supplied duration can overflow
/// the clock into a skipped cooldown, and the REST client bounds the
/// `retry_after` it parses and reports from the same constant, keeping
/// admission and `Error::ProviderRateLimited` in lockstep. A provider
/// cooldown longer than the ceiling is deliberately shortened to it rather
/// than blocking admission for a duration the caller cannot distinguish
/// from permanent; the ceiling matches the longest parsed `Retry-After`
/// the REST client will act on.
pub(crate) const MAX_COOLDOWN: Duration = Duration::from_hours(24);

/// Identifies one provider REST rate-limit budget.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum RateLimitKind {
    /// The dedicated `/api/History/retrieveBars` budget.
    History,
    /// The budget shared by all other authenticated REST endpoints.
    General,
}

impl fmt::Display for RateLimitKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::History => formatter.write_str("history"),
            Self::General => formatter.write_str("general"),
        }
    }
}

/// One rolling-window request limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RateLimit {
    max_requests: usize,
    window: Duration,
}

impl RateLimit {
    /// Creates a non-zero rolling-window request limit.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] when `max_requests` or `window` is
    /// zero, or when the duration cannot be represented by a monotonic clock.
    pub fn new(max_requests: usize, window: Duration) -> Result<Self, Error> {
        if max_requests == 0
            || window.is_zero()
            || std::time::Instant::now().checked_add(window).is_none()
        {
            return Err(Error::Configuration(ConfigurationError::InvalidRateLimit));
        }
        Ok(Self {
            max_requests,
            window,
        })
    }

    const fn trusted(max_requests: usize, window: Duration) -> Self {
        Self {
            max_requests,
            window,
        }
    }

    /// Returns the maximum admitted requests in the rolling window.
    #[must_use]
    pub const fn max_requests(self) -> usize {
        self.max_requests
    }

    /// Returns the rolling-window duration.
    #[must_use]
    pub const fn window(self) -> Duration {
        self.window
    }
}

/// Rate limits applied to authenticated `ProjectX` REST requests.
///
/// [`Default`] uses the provider's documented limits: 50 history requests per
/// 30 seconds and 200 other authenticated requests per 60 seconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RateLimitConfig {
    history: RateLimit,
    general: RateLimit,
    exit_reserve: usize,
}

impl RateLimitConfig {
    /// Creates configuration from dedicated history and general limits.
    #[must_use]
    pub const fn new(history: RateLimit, general: RateLimit) -> Self {
        Self {
            history,
            general,
            exit_reserve: 0,
        }
    }

    /// Reserves general-window capacity for cancellation and position closes.
    ///
    /// Ordinary requests are admitted only below `general.max_requests() -
    /// reserve`; exits may use the full general limit. Both classes count
    /// against the same rolling window and provider cooldown. The default is zero.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] when `reserve` is not smaller than
    /// the general request limit, which must retain ordinary capacity.
    pub fn with_exit_reserve(mut self, reserve: usize) -> Result<Self, Error> {
        if reserve >= self.general.max_requests {
            return Err(Error::Configuration(
                ConfigurationError::ExitReserveExhaustsCapacity,
            ));
        }
        self.exit_reserve = reserve;
        Ok(self)
    }

    /// Returns the general-window capacity reserved for cancellation and closes.
    #[must_use]
    pub const fn exit_reserve(self) -> usize {
        self.exit_reserve
    }

    /// Returns the history endpoint limit.
    #[must_use]
    pub const fn history(self) -> RateLimit {
        self.history
    }

    /// Returns the limit for all other authenticated endpoints.
    #[must_use]
    pub const fn general(self) -> RateLimit {
        self.general
    }

    pub(crate) const fn limit(self, kind: RateLimitKind) -> RateLimit {
        match kind {
            RateLimitKind::History => self.history,
            RateLimitKind::General => self.general,
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            history: RateLimit::trusted(DEFAULT_HISTORY_REQUESTS, DEFAULT_HISTORY_WINDOW),
            general: RateLimit::trusted(DEFAULT_GENERAL_REQUESTS, DEFAULT_GENERAL_WINDOW),
            exit_reserve: 0,
        }
    }
}

pub(crate) struct RateLimits {
    config: Option<RateLimitConfig>,
    history: Option<WindowLimiter>,
    general: Option<WindowLimiter>,
}

impl RateLimits {
    pub(crate) fn new(config: Option<RateLimitConfig>) -> Self {
        let history = config.map(|limits| WindowLimiter::new(limits.history, 0));
        let general = config.map(|limits| WindowLimiter::new(limits.general, limits.exit_reserve));
        Self {
            config,
            history,
            general,
        }
    }

    pub(crate) fn config(&self) -> Option<RateLimitConfig> {
        self.config.map(|mut config| {
            if let Some(limiter) = &self.general {
                config.exit_reserve = limiter.state.lock().exit_reserve;
            }
            config
        })
    }

    pub(crate) fn set_exit_reserve(&self, reserve: usize) -> Result<(), Error> {
        let limiter = self.general.as_ref().ok_or_else(|| {
            Error::Configuration(ConfigurationError::ExitReserveRequiresRateLimits)
        })?;
        let mut state = limiter.state.lock();
        if reserve >= limiter.limit.max_requests {
            return Err(Error::Configuration(
                ConfigurationError::ExitReserveExhaustsCapacity,
            ));
        }
        let changed = state.exit_reserve != reserve;
        state.exit_reserve = reserve;
        drop(state);
        if changed {
            limiter.policy_changed.notify_waiters();
        }
        Ok(())
    }

    pub(crate) fn limit(&self, kind: RateLimitKind) -> RateLimit {
        self.config.unwrap_or_default().limit(kind)
    }

    pub(crate) async fn wait(&self, kind: RateLimitKind) {
        let Some(limiter) = self.limiter(kind) else {
            return;
        };
        loop {
            let changed = limiter.policy_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match limiter.try_acquire(false) {
                Ok(()) => return,
                Err(retry_after) => {
                    tokio::select! {
                        () = &mut changed => {},
                        () = sleep(retry_after) => {},
                    }
                }
            }
        }
    }

    pub(crate) fn try_acquire(&self, kind: RateLimitKind) -> Result<(), Duration> {
        self.limiter(kind)
            .map_or(Ok(()), |limiter| limiter.try_acquire(false))
    }

    pub(crate) fn try_acquire_exit(&self) -> Result<(), Duration> {
        self.general
            .as_ref()
            .map_or(Ok(()), |limiter| limiter.try_acquire(true))
    }

    pub(crate) fn cool_down(&self, kind: RateLimitKind, retry_after: Duration) {
        if let Some(limiter) = self.limiter(kind) {
            limiter.cool_down(retry_after);
        }
    }

    fn limiter(&self, kind: RateLimitKind) -> Option<&WindowLimiter> {
        match kind {
            RateLimitKind::History => self.history.as_ref(),
            RateLimitKind::General => self.general.as_ref(),
        }
    }
}

struct WindowLimiter {
    limit: RateLimit,
    policy_changed: Notify,
    state: Mutex<WindowState>,
}

impl WindowLimiter {
    fn new(limit: RateLimit, exit_reserve: usize) -> Self {
        Self {
            limit,
            policy_changed: Notify::new(),
            state: Mutex::new(WindowState::with_capacity(
                limit.max_requests.min(MAX_INITIAL_CAPACITY),
                exit_reserve,
            )),
        }
    }

    fn try_acquire(&self, exit: bool) -> Result<(), Duration> {
        let mut state = self.state.lock();
        let capacity = if exit {
            self.limit.max_requests
        } else {
            self.limit.max_requests - state.exit_reserve
        };
        let now = Instant::now();
        state.expire(now, self.limit.window);
        let retry_after = state.retry_after(now, self.limit.window, capacity);
        if retry_after.is_zero() {
            state.admitted.push_back(now);
            Ok(())
        } else {
            Err(retry_after)
        }
    }

    fn cool_down(&self, retry_after: Duration) {
        let mut state = self.state.lock();
        let deadline = saturating_deadline(Instant::now(), retry_after.min(MAX_COOLDOWN));
        if state
            .cooldown_until
            .is_none_or(|current| deadline > current)
        {
            state.cooldown_until = Some(deadline);
        }
    }
}

struct WindowState {
    exit_reserve: usize,
    admitted: VecDeque<Instant>,
    cooldown_until: Option<Instant>,
}

impl WindowState {
    fn with_capacity(capacity: usize, exit_reserve: usize) -> Self {
        Self {
            exit_reserve,
            admitted: VecDeque::with_capacity(capacity),
            cooldown_until: None,
        }
    }

    fn expire(&mut self, now: Instant, window: Duration) {
        while self
            .admitted
            .front()
            .is_some_and(|admitted| now.saturating_duration_since(*admitted) >= window)
        {
            self.admitted.pop_front();
        }
        if self.cooldown_until.is_some_and(|deadline| deadline <= now) {
            self.cooldown_until = None;
        }
    }

    fn retry_after(&self, now: Instant, window: Duration, capacity: usize) -> Duration {
        let cooldown = self.cooldown_until.map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(now)
        });
        let capacity_delay = if self.admitted.len() < capacity {
            Duration::ZERO
        } else {
            // Exits may fill beyond ordinary capacity; admission requires
            // enough aggregate entries to expire, not only the oldest one.
            self.admitted
                .get(self.admitted.len() - capacity)
                .map_or(window, |admitted| {
                    admitted
                        .checked_add(window)
                        .map_or(window, |deadline| deadline.saturating_duration_since(now))
                })
        };
        cooldown.max(capacity_delay)
    }
}

/// Returns `now + duration`, or the latest instant the monotonic clock can
/// represent when the addition overflows, so a cooldown the clock cannot
/// express still blocks admission to the end of the clock instead of being
/// dropped.
fn saturating_deadline(now: Instant, duration: Duration) -> Instant {
    if let Some(deadline) = now.checked_add(duration) {
        return deadline;
    }
    // The full duration overflows, so a representable offset exists below
    // it. Binary-search nanosecond offsets capped at `u64::MAX`, the largest
    // `Duration::from_nanos` can express. Every assignment to `low` follows
    // a successful clock probe, so `deadline` is only ever set from a
    // representable offset; the initial zero offset is representable by
    // definition and can only survive the search when not even the next
    // nanosecond is. That terminal case yields a no-op cooldown —
    // `expire` clears it on the next admission attempt — because no future
    // instant exists to block until.
    let mut low = 0_u64;
    let mut high = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
    let mut deadline = now;
    while high - low > 1 {
        let mid = u64::midpoint(low, high);
        if let Some(probe) = now.checked_add(Duration::from_nanos(mid)) {
            low = mid;
            deadline = probe;
        } else {
            high = mid;
        }
    }
    deadline
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::{Instant, advance};

    use super::*;

    fn limits(requests: usize, window: Duration) -> Arc<RateLimits> {
        let limit = RateLimit::new(requests, window)
            .unwrap_or_else(|error| panic!("test rate limit must be valid: {error}"));
        Arc::new(RateLimits::new(Some(RateLimitConfig::new(limit, limit))))
    }

    #[tokio::test(start_paused = true)]
    async fn wait_resumes_when_the_rolling_window_releases_capacity() {
        let window = Duration::from_secs(10);
        let limits = limits(1, window);
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));

        let started = Instant::now();
        let waiting_limits = Arc::clone(&limits);
        let waiter = tokio::spawn(async move {
            waiting_limits.wait(RateLimitKind::General).await;
            Instant::now()
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        advance(window).await;
        let admitted = waiter
            .await
            .unwrap_or_else(|error| panic!("waiting task must finish: {error}"));
        assert_eq!(admitted.saturating_duration_since(started), window);
    }

    #[tokio::test(start_paused = true)]
    async fn provider_cooldown_blocks_admission_until_its_deadline() {
        let cooldown = Duration::from_secs(15);
        let limits = limits(2, Duration::from_secs(30));
        limits.cool_down(RateLimitKind::General, cooldown);

        assert_eq!(limits.try_acquire(RateLimitKind::General), Err(cooldown));
        advance(cooldown).await;
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn an_oversized_cooldown_is_clamped_not_skipped() {
        let limits = limits(2, Duration::from_secs(30));
        limits.cool_down(RateLimitKind::General, Duration::MAX);

        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_hours(24)),
            "an unrepresentable provider cooldown must still block admission"
        );
    }

    #[test]
    fn an_unrepresentable_deadline_search_terminates_and_still_blocks() {
        let now = Instant::now();
        let deadline = saturating_deadline(now, Duration::MAX);
        // The upper bound separates this from the fast path: a successful
        // `checked_add(Duration::MAX)` would exceed the search's nanosecond
        // cap, so passing both bounds proves the capped search ran.
        let offset = deadline.saturating_duration_since(now);
        assert!(
            offset > Duration::from_hours(24) && offset <= Duration::from_nanos(u64::MAX),
            "the saturated deadline must still block admission within the \
             searched nanosecond domain, found {offset:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn rolling_window_expires_staggered_attempts_individually() {
        let window = Duration::from_secs(10);
        let limits = limits(2, window);
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        advance(Duration::from_secs(5)).await;
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));

        advance(Duration::from_secs(5)).await;
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(5))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_waiter_leaves_no_ghost_reservation() {
        let window = Duration::from_secs(10);
        let limits = limits(1, window);
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        let waiting_limits = Arc::clone(&limits);
        let waiter = tokio::spawn(async move {
            waiting_limits.wait(RateLimitKind::General).await;
        });
        tokio::task::yield_now().await;
        waiter.abort();

        advance(window).await;
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn default_history_budget_reaches_its_limit_before_throttling() {
        let limits = Arc::new(RateLimits::new(Some(RateLimitConfig::default())));
        for request_number in 1..=50 {
            assert_eq!(
                limits.try_acquire(RateLimitKind::History),
                Ok(()),
                "history request {request_number} must be admitted"
            );
        }
        assert_eq!(
            limits.try_acquire(RateLimitKind::History),
            Err(Duration::from_secs(30))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn default_general_budget_reaches_its_limit_before_throttling() {
        let limits = Arc::new(RateLimits::new(Some(RateLimitConfig::default())));
        for request_number in 1..=200 {
            assert_eq!(
                limits.try_acquire(RateLimitKind::General),
                Ok(()),
                "general request {request_number} must be admitted"
            );
        }
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_mins(1))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn later_provider_cooldown_cannot_be_shortened() {
        let limits = limits(200, Duration::from_mins(1));
        limits.cool_down(RateLimitKind::General, Duration::from_secs(30));
        limits.cool_down(RateLimitKind::General, Duration::from_secs(5));
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(30))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn admission_uses_the_later_of_window_and_provider_cooldown() {
        let limits = limits(1, Duration::from_secs(20));
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        limits.cool_down(RateLimitKind::General, Duration::from_secs(5));
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(20))
        );
    }

    #[test]
    fn history_and_general_budgets_are_independent() {
        let limits = limits(1, Duration::from_secs(30));
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        assert_eq!(limits.try_acquire(RateLimitKind::History), Ok(()));
        assert!(limits.try_acquire(RateLimitKind::General).is_err());
        assert!(limits.try_acquire(RateLimitKind::History).is_err());
    }

    #[test]
    fn public_defaults_match_the_provider_contract() {
        let config = RateLimitConfig::default();
        assert_eq!(config.history().max_requests(), 50);
        assert_eq!(config.history().window(), Duration::from_secs(30));
        assert_eq!(config.general().max_requests(), 200);
        assert_eq!(config.general().window(), Duration::from_mins(1));
    }

    #[test]
    fn public_rate_limit_rejects_zero_values() {
        assert!(RateLimit::new(0, Duration::from_secs(1)).is_err());
        assert!(RateLimit::new(1, Duration::ZERO).is_err());
    }

    fn reserved_limits() -> Arc<RateLimits> {
        let config = RateLimitConfig::new(
            RateLimit::new(5, Duration::from_secs(10)).unwrap_or_else(|error| panic!("{error}")),
            RateLimit::new(5, Duration::from_secs(10)).unwrap_or_else(|error| panic!("{error}")),
        )
        .with_exit_reserve(2)
        .unwrap_or_else(|error| panic!("{error}"));
        Arc::new(RateLimits::new(Some(config)))
    }

    #[tokio::test(start_paused = true)]
    async fn exit_reserve_keeps_ordinary_waiters_below_the_shared_aggregate_cap() {
        let limits = reserved_limits();
        for _ in 0..3 {
            assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        }
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(10))
        );
        let waiting_limits = Arc::clone(&limits);
        let waiter = tokio::spawn(async move {
            waiting_limits.wait(RateLimitKind::General).await;
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(10)));
        waiter.abort();
        assert!(waiter.await.is_err_and(|error| error.is_cancelled()));
        advance(Duration::from_secs(10)).await;
        for _ in 0..3 {
            assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        }
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(10))
        );
        assert_eq!(limits.try_acquire_exit(), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn ordinary_retry_after_waits_for_enough_aggregate_entries_to_expire() {
        let limits = reserved_limits();
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        advance(Duration::from_secs(1)).await;
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        advance(Duration::from_secs(1)).await;
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        advance(Duration::from_secs(1)).await;
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        advance(Duration::from_secs(1)).await;
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(6)));
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(8))
        );
        advance(Duration::from_secs(6)).await;
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(2))
        );
        advance(Duration::from_secs(2)).await;
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(1))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_exit_admission_never_exceeds_shared_capacity() {
        let limits = reserved_limits();
        for _ in 0..3 {
            assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        }
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..20 {
            let clone = Arc::clone(&limits);
            tasks.spawn(async move { clone.try_acquire_exit() });
        }
        let mut accepted = 0;
        let mut refused = 0;
        while let Some(result) = tasks.join_next().await {
            match result.unwrap_or_else(|error| panic!("{error}")) {
                Ok(()) => accepted += 1,
                Err(delay) => {
                    assert_eq!(delay, Duration::from_secs(10));
                    refused += 1;
                }
            }
        }
        assert_eq!(accepted, 2);
        assert_eq!(refused, 18);
    }

    #[tokio::test(start_paused = true)]
    async fn provider_cooldown_blocks_ordinary_and_reserved_capacity() {
        let limits = reserved_limits();
        limits.cool_down(RateLimitKind::General, Duration::from_secs(15));
        assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(15)));
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(15))
        );
        advance(Duration::from_secs(14)).await;
        assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(1)));
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(1))
        );
        advance(Duration::from_secs(1)).await;
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
    }

    #[test]
    fn exit_reserve_retains_ordinary_capacity_and_defaults_to_zero() {
        let config = RateLimitConfig::default();
        assert_eq!(config.exit_reserve(), 0);
        assert_eq!(
            RateLimitConfig::new(config.history(), config.general()).exit_reserve(),
            0
        );
        assert_eq!(
            config
                .with_exit_reserve(199)
                .unwrap_or_else(|error| panic!("{error}"))
                .exit_reserve(),
            199
        );
        assert!(matches!(
            config.with_exit_reserve(200),
            Err(Error::Configuration(_))
        ));
        assert!(matches!(
            config.with_exit_reserve(usize::MAX),
            Err(Error::Configuration(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_release_wakes_all_registered_waiters_without_advancing_time() {
        let limits = reserved_limits();
        for _ in 0..3 {
            assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        }
        let first = limits.wait(RateLimitKind::General);
        let second = limits.wait(RateLimitKind::General);
        tokio::pin!(first, second);
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        assert!(futures_util::poll!(second.as_mut()).is_pending());
        let started = Instant::now();
        let clone = Arc::clone(&limits);
        clone
            .set_exit_reserve(0)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(futures_util::poll!(first.as_mut()).is_ready());
        assert!(futures_util::poll!(second.as_mut()).is_ready());
        assert_eq!(Instant::now(), started);
        assert_eq!(limits.config().map(RateLimitConfig::exit_reserve), Some(0));
        assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(10)));
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_growth_rechecks_waiters_and_preserves_spent_capacity() {
        let limits = reserved_limits();
        for _ in 0..3 {
            assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        }
        let read = limits.wait(RateLimitKind::General);
        tokio::pin!(read);
        assert!(futures_util::poll!(read.as_mut()).is_pending());
        limits
            .set_exit_reserve(0)
            .unwrap_or_else(|error| panic!("{error}"));
        limits
            .set_exit_reserve(4)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(futures_util::poll!(read.as_mut()).is_pending());
        assert_eq!(limits.config().map(RateLimitConfig::exit_reserve), Some(4));
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(10))
        );
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(10)));
        limits
            .set_exit_reserve(0)
            .unwrap_or_else(|error| panic!("{error}"));
        limits
            .set_exit_reserve(4)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(10)));
        limits
            .set_exit_reserve(0)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(futures_util::poll!(read.as_mut()).is_pending());
        advance(Duration::from_secs(10)).await;
        assert!(futures_util::poll!(read.as_mut()).is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_changes_preserve_timestamps_cooldown_and_invalid_policy_state() {
        let limits = reserved_limits();
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        advance(Duration::from_secs(2)).await;
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        limits.cool_down(RateLimitKind::General, Duration::from_secs(3));
        for reserve in [4, 0, 2] {
            limits
                .set_exit_reserve(reserve)
                .unwrap_or_else(|error| panic!("{error}"));
            assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(3)));
        }
        for reserve in [5, usize::MAX] {
            assert!(matches!(
                limits.set_exit_reserve(reserve),
                Err(Error::Configuration(_))
            ));
            assert_eq!(limits.config().map(RateLimitConfig::exit_reserve), Some(2));
            assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(3)));
        }
        advance(Duration::from_secs(3)).await;
        for _ in 0..3 {
            assert_eq!(limits.try_acquire_exit(), Ok(()));
        }
        assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(5)));
        advance(Duration::from_secs(5)).await;
        assert_eq!(limits.try_acquire_exit(), Ok(()));
        assert_eq!(limits.try_acquire_exit(), Err(Duration::from_secs(2)));
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_waiter_rechecks_each_replacement_including_release_then_growth() {
        let limits = reserved_limits();
        limits
            .set_exit_reserve(4)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(limits.try_acquire(RateLimitKind::General), Ok(()));
        let read = limits.wait(RateLimitKind::General);
        tokio::pin!(read);
        assert!(futures_util::poll!(read.as_mut()).is_pending());
        limits
            .set_exit_reserve(0)
            .unwrap_or_else(|error| panic!("{error}"));
        limits
            .set_exit_reserve(4)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(futures_util::poll!(read.as_mut()).is_pending());
        limits
            .set_exit_reserve(3)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(futures_util::poll!(read.as_mut()).is_ready());
        assert_eq!(
            limits.try_acquire(RateLimitKind::General),
            Err(Duration::from_secs(10))
        );
    }
}
