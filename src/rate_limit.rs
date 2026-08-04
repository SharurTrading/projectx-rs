// SPDX-FileCopyrightText: 2026 Kevin Monaghan
// SPDX-License-Identifier: MIT-0

//! Provider REST rate-limit configuration and shared admission control.

use std::{collections::VecDeque, fmt, time::Duration};

use parking_lot::Mutex;
use tokio::time::{Instant, sleep};

use crate::Error;

const DEFAULT_HISTORY_REQUESTS: usize = 50;
const DEFAULT_HISTORY_WINDOW: Duration = Duration::from_secs(30);
const DEFAULT_GENERAL_REQUESTS: usize = 200;
const DEFAULT_GENERAL_WINDOW: Duration = Duration::from_mins(1);
const MAX_INITIAL_CAPACITY: usize = 1_024;

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
            return Err(Error::Configuration(
                "rate limits require a positive request count and representable window".to_owned(),
            ));
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
}

impl RateLimitConfig {
    /// Creates configuration from dedicated history and general limits.
    #[must_use]
    pub const fn new(history: RateLimit, general: RateLimit) -> Self {
        Self { history, general }
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
        let history = config.map(|limits| WindowLimiter::new(limits.history));
        let general = config.map(|limits| WindowLimiter::new(limits.general));
        Self {
            config,
            history,
            general,
        }
    }

    pub(crate) const fn config(&self) -> Option<RateLimitConfig> {
        self.config
    }

    pub(crate) fn limit(&self, kind: RateLimitKind) -> RateLimit {
        self.config.unwrap_or_default().limit(kind)
    }

    pub(crate) async fn wait(&self, kind: RateLimitKind) {
        let Some(limiter) = self.limiter(kind) else {
            return;
        };
        loop {
            match limiter.try_acquire(Instant::now()) {
                Ok(()) => return,
                Err(retry_after) => sleep(retry_after).await,
            }
        }
    }

    pub(crate) fn try_acquire(&self, kind: RateLimitKind) -> Result<(), Duration> {
        self.limiter(kind)
            .map_or(Ok(()), |limiter| limiter.try_acquire(Instant::now()))
    }

    pub(crate) fn cool_down(&self, kind: RateLimitKind, retry_after: Duration) {
        if let Some(limiter) = self.limiter(kind) {
            limiter.cool_down(Instant::now(), retry_after);
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
    state: Mutex<WindowState>,
}

impl WindowLimiter {
    fn new(limit: RateLimit) -> Self {
        Self {
            limit,
            state: Mutex::new(WindowState::with_capacity(
                limit.max_requests.min(MAX_INITIAL_CAPACITY),
            )),
        }
    }

    fn try_acquire(&self, now: Instant) -> Result<(), Duration> {
        let mut state = self.state.lock();
        state.expire(now, self.limit.window);
        let retry_after = state.retry_after(now, self.limit);
        if retry_after.is_zero() {
            state.admitted.push_back(now);
            Ok(())
        } else {
            Err(retry_after)
        }
    }

    fn cool_down(&self, now: Instant, retry_after: Duration) {
        let Some(deadline) = now.checked_add(retry_after) else {
            return;
        };
        let mut state = self.state.lock();
        if state
            .cooldown_until
            .is_none_or(|current| deadline > current)
        {
            state.cooldown_until = Some(deadline);
        }
    }
}

struct WindowState {
    admitted: VecDeque<Instant>,
    cooldown_until: Option<Instant>,
}

impl WindowState {
    fn with_capacity(capacity: usize) -> Self {
        Self {
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

    fn retry_after(&self, now: Instant, limit: RateLimit) -> Duration {
        let cooldown = self.cooldown_until.map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(now)
        });
        let capacity = if self.admitted.len() < limit.max_requests {
            Duration::ZERO
        } else {
            self.admitted.front().map_or(limit.window, |oldest| {
                oldest
                    .checked_add(limit.window)
                    .map_or(limit.window, |deadline| {
                        deadline.saturating_duration_since(now)
                    })
            })
        };
        cooldown.max(capacity)
    }
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
}
