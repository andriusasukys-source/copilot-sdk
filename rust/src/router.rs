use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc};
use tracing::warn;

use crate::jsonrpc::{JsonRpcNotification, JsonRpcRequest};
use crate::types::{SessionEventNotification, SessionId};

/// Per-session channels created by the router during session registration.
pub(crate) struct SessionChannels {
    /// Filtered `session.event` notifications for this session.
    pub(crate) notifications: mpsc::UnboundedReceiver<SessionEventNotification>,
    /// Filtered JSON-RPC requests (tool.call, userInput.request, etc.) for this session.
    pub(crate) requests: mpsc::UnboundedReceiver<JsonRpcRequest>,
}

struct SessionSenders {
    notifications: mpsc::UnboundedSender<SessionEventNotification>,
    requests: mpsc::UnboundedSender<JsonRpcRequest>,
}

#[derive(Default)]
struct SessionRouterState {
    sessions: HashMap<SessionId, SessionSenders>,
}

impl SessionRouterState {
    fn register(&mut self, session_id: &SessionId, senders: SessionSenders) {
        self.sessions.insert(session_id.clone(), senders);
    }

    fn route_notification(&mut self, session_id: &str, notification: SessionEventNotification) {
        if let Some(sender) = self.sessions.get(session_id) {
            let _ = sender.notifications.send(notification);
        }
    }

    fn route_request(&mut self, request: JsonRpcRequest) {
        let Some(session_id) = request
            .params
            .as_ref()
            .and_then(|p| p.get("sessionId"))
            .and_then(|v| v.as_str())
        else {
            warn!(method = %request.method, "request missing sessionId");
            return;
        };
        if let Some(sender) = self.sessions.get(session_id) {
            let _ = sender.requests.send(request);
            return;
        }
        warn!(
            session_id = session_id,
            method = %request.method,
            "request for unregistered session"
        );
    }
}

/// Routes notifications and requests by sessionId to per-session channels.
///
/// Internal to the SDK — consumers interact via `Client::register_session()`.
pub(crate) struct SessionRouter {
    state: Arc<Mutex<SessionRouterState>>,
}

impl SessionRouter {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SessionRouterState::default())),
        }
    }

    /// Register a session to receive filtered events and requests.
    pub(crate) fn register(&self, session_id: &SessionId) -> SessionChannels {
        let (notif_tx, notif_rx) = mpsc::unbounded_channel();
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        self.state.lock().register(
            session_id,
            SessionSenders {
                notifications: notif_tx,
                requests: req_tx,
            },
        );
        SessionChannels {
            notifications: notif_rx,
            requests: req_rx,
        }
    }

    /// Unregister a session, dropping its channels.
    pub(crate) fn unregister(&self, session_id: &SessionId) {
        self.state.lock().sessions.remove(session_id.as_str());
    }

    /// Snapshot every currently-registered session ID.
    ///
    /// Used by [`Client::stop`](crate::Client::stop) to iterate active
    /// sessions for cooperative shutdown without holding the router lock
    /// across `.await`.
    pub(crate) fn session_ids(&self) -> Vec<SessionId> {
        self.state.lock().sessions.keys().cloned().collect()
    }

    /// Drop all registered session channels.
    ///
    /// Used by [`Client::force_stop`](crate::Client::force_stop) to release
    /// per-session state without waiting for graceful unregistration.
    pub(crate) fn clear(&self) {
        self.state.lock().sessions.clear();
    }

    /// Spawn the notification and request routing tasks.
    ///
    /// Called exactly once during [`Client::from_streams`]. Takes the
    /// notification broadcast and request channel from the Client. If
    /// `request_rx` is `None` (already taken by `take_request_rx()`), only
    /// notification routing is available.
    pub(crate) fn start(
        &self,
        notification_tx: &broadcast::Sender<JsonRpcNotification>,
        request_rx: &Mutex<Option<mpsc::UnboundedReceiver<JsonRpcRequest>>>,
    ) {
        // Notification routing task
        let state = self.state.clone();
        let mut notif_rx = notification_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match notif_rx.recv().await {
                    Ok(notification) => {
                        if notification.method != "session.event" {
                            continue;
                        }
                        let Some(ref params) = notification.params else {
                            continue;
                        };
                        let Some(session_id) = params.get("sessionId").and_then(|v| v.as_str())
                        else {
                            continue;
                        };

                        match serde_json::from_value::<SessionEventNotification>(params.clone()) {
                            Ok(event_notification) => {
                                state
                                    .lock()
                                    .route_notification(session_id, event_notification);
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    session_id = session_id,
                                    "failed to deserialize session event notification"
                                );
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(missed = n, "notification router lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        // Request routing task (if request_rx is available)
        if let Some(mut rx) = request_rx.lock().take() {
            let state = self.state.clone();
            tokio::spawn(async move {
                while let Some(request) = rx.recv().await {
                    state.lock().route_request(request);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::jsonrpc::JsonRpcRequest;

    fn make_notification(session_id: &str, kind: &str) -> SessionEventNotification {
        let value = json!({
            "sessionId": session_id,
            "event": {
                "id": "evt-id",
                "timestamp": "1970-01-01T00:00:00Z",
                "parentId": null,
                "type": kind,
                "data": {},
            },
        });
        serde_json::from_value(value).expect("valid session event notification")
    }

    fn make_request(id: u64, session_id: &str, method: &str) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.to_string(),
            params: Some(json!({ "sessionId": session_id })),
        }
    }

    #[test]
    fn drops_unknown_session_notifications() {
        let router = SessionRouter::new();
        router
            .state
            .lock()
            .route_notification("ghost", make_notification("ghost", "session.start"));

        let channels = router.register(&SessionId::from("ghost"));
        assert!(channels.notifications.is_empty());
    }

    #[test]
    fn drops_unknown_session_requests() {
        let router = SessionRouter::new();
        router
            .state
            .lock()
            .route_request(make_request(1, "ghost", "userInput.request"));

        let channels = router.register(&SessionId::from("ghost"));
        assert!(channels.requests.is_empty());
    }

    #[test]
    fn routes_registered_session_messages() {
        let router = SessionRouter::new();
        let sid = SessionId::from("remote");
        let mut channels = router.register(&sid);

        {
            let mut state = router.state.lock();
            state.route_notification("remote", make_notification("remote", "evt"));
            state.route_request(make_request(1, "remote", "userInput.request"));
        }

        let notification = channels.notifications.try_recv().expect("notification");
        assert_eq!(notification.event.event_type, "evt");
        let request = channels.requests.try_recv().expect("request");
        assert_eq!(request.id, 1);
    }
}
