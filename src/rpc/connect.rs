use std::error::Error as StdError;
use std::future::Future;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use http::Uri;
use hyper::client::HttpConnector;
use tokio::net::TcpStream;
use tokio::time;
use tower_service::Service;

#[derive(Debug, Clone)]
pub struct Connector {
    http_connector: HttpConnector,
}

impl Default for Connector {
    fn default() -> Self {
        Self {
            http_connector: HttpConnector::new(),
        }
    }
}

impl Service<Uri> for Connector {
    type Response = TcpStream;
    type Error = Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http_connector
            .poll_ready(cx)
            .map_err(|err| Error::new(ErrorKind::Other, err))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let mut connector = self.clone();

        Box::pin(async move {
            let stream = loop {
                match connector.http_connector.call(req.clone()).await {
                    Err(err) => {
                        if source_is_connect_refused(&err) {
                            time::sleep(Duration::from_secs(1)).await;

                            continue;
                        }

                        return Err(Error::new(ErrorKind::Other, err));
                    }

                    Ok(stream) => break stream,
                }
            };

            Ok(stream)
        })
    }
}

fn source_is_connect_refused(mut err: &(dyn StdError + 'static)) -> bool {
    while let Some(source) = err.source() {
        if let Some(io_err) = source.downcast_ref::<Error>() {
            return io_err.kind() == ErrorKind::ConnectionRefused;
        }

        err = source;
    }

    false
}
