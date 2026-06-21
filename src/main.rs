use async_trait::async_trait;
use pingora::prelude::*;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::RwLock;

use kind::kind_service_client::KindServiceClient;
use kind::{PutRequest, WatchRequest};
use serde::{Deserialize, Serialize};

pub mod kind {
    tonic::include_proto!("kind");
}

pub type RoutingTable = Arc<RwLock<HashMap<String, Vec<String>>>>;
pub type MetricsTable = Arc<RwLock<HashMap<String, AtomicUsize>>>;

pub struct VortexRouter {
    pub routing_table: RoutingTable,
    pub metrics_table: MetricsTable,
    pub request_counter: AtomicUsize,
}

#[async_trait]
impl ProxyHttp for VortexRouter {
    type CTX = ();
    
    fn new_ctx(&self) -> () {
        ()
    }

    async fn upstream_peer(&self, session: &mut Session, _ctx: &mut ()) -> Result<Box<HttpPeer>> {
        let host_header = session.get_header("host");
        let host = host_header.and_then(|v| v.to_str().ok()).unwrap_or("");
        
        let client_id = host.split('.').next().unwrap_or("");
        
        if client_id.is_empty() {
            let _ = session.respond_error(502).await;
            return Err(pingora::Error::explain(
                pingora::ErrorType::Custom("No client ID"),
                "No client ID",
            ));
        }

        let ips = {
            let table = self.routing_table.read().await;
            table.get(client_id).cloned()
        };

        let ip = match ips {
            Some(list) if !list.is_empty() => {
                let idx = self.request_counter.fetch_add(1, Ordering::Relaxed) % list.len();
                list[idx].clone()
            }
            _ => {
                let _ = session.respond_error(502).await;
                return Err(pingora::Error::explain(
                    pingora::ErrorType::Custom("No backends"),
                    "No backends available",
                ));
            }
        };

        {
            let table = self.metrics_table.read().await;
            if let Some(counter) = table.get(client_id) {
                counter.fetch_add(1, Ordering::Relaxed);
            } else {
                drop(table);
                let mut write_table = self.metrics_table.write().await;
                write_table
                    .entry(client_id.to_string())
                    .or_insert_with(|| AtomicUsize::new(0))
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        let peer = HttpPeer::new(ip, false, "".to_string());
        Ok(Box::new(peer))
    }
}

#[derive(Deserialize)]
struct RouteUpdate {
    client_id: String,
    ips: Vec<String>,
}

#[derive(Serialize)]
struct MetricUpdate<'a> {
    client_id: &'a str,
    current_rps: usize,
}

pub async fn run_kind_db_watcher(table: RoutingTable) {
    if let Ok(mut client) = KindServiceClient::connect("http://localhost:50051").await {
        let req = WatchRequest {
            prefix: "router:".to_string(),
        };
        if let Ok(response) = client.watch(tonic::Request::new(req)).await {
            let mut stream = response.into_inner();
            while let Ok(Some(res)) = stream.message().await {
                if res.operation_type == "PUT" {
                    if let Ok(update) = serde_json::from_slice::<RouteUpdate>(&res.new_value) {
                        let mut write_guard = table.write().await;
                        write_guard.insert(update.client_id, update.ips);
                    }
                }
            }
        }
    }
}

pub async fn run_metrics_publisher(metrics: MetricsTable) {
    if let Ok(mut client) = KindServiceClient::connect("http://localhost:50051").await {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let mut updates = Vec::new();
            {
                let table = metrics.read().await;
                for (client_id, counter) in table.iter() {
                    let rps = counter.swap(0, Ordering::Relaxed);
                    if rps > 0 {
                        updates.push((client_id.clone(), rps));
                    }
                }
            }

            for (client_id, current_rps) in updates {
                let payload = MetricUpdate {
                    client_id: &client_id,
                    current_rps,
                };
                if let Ok(value) = serde_json::to_vec(&payload) {
                    let req = PutRequest {
                        key: format!("vortex:metrics:{}", client_id),
                        value,
                    };
                    let _ = client.put(tonic::Request::new(req)).await;
                }
            }
        }
    }
}

fn main() {
    let mut server = Server::new(None).unwrap();
    server.bootstrap();

    let routing_table: RoutingTable = Arc::new(RwLock::new(HashMap::new()));
    let metrics_table: MetricsTable = Arc::new(RwLock::new(HashMap::new()));

    let rt_clone = routing_table.clone();
    tokio::spawn(async move {
        run_kind_db_watcher(rt_clone).await;
    });

    let mt_clone = metrics_table.clone();
    tokio::spawn(async move {
        run_metrics_publisher(mt_clone).await;
    });

    let router = VortexRouter {
        routing_table,
        metrics_table,
        request_counter: AtomicUsize::new(0),
    };

    let mut proxy_service = pingora::proxy::http_proxy_service(&server.configuration, router);
    proxy_service.add_tcp("0.0.0.0:8000");

    server.add_service(proxy_service);
    server.run_forever();
}