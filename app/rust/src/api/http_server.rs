use crate::api::stream::Dart2RustStreamReceiver;
use crate::frb_generated::StreamSink;
use bytes::Bytes;
use flutter_rust_bridge::frb;
use localsend::http::dto::{ProtocolType, RegisterDto};
use localsend::http::dto_v2::{ProtocolTypeV2, RegisterDtoV2};
use localsend::http::server::common::save::FileUploadTarget;
use localsend::http::server::internal::{InternalConfig, InternalEvent};
use localsend::http::server::v2::{PrepareUploadDecisionV2, ServerEventV2, SessionEndReasonV2};
use localsend::http::server::web::{WebSendConfig, WebSendEvent, WebSendI18n};
use localsend::http::server::{ServerConfigV2, TlsConfig};
use localsend::http::state::ClientInfo;
use localsend::model::transfer::{FileContent, FileDto};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex as StdMutex;
use tokio::sync::{Mutex, mpsc, oneshot};

const EVENT_CHANNEL_CAPACITY: usize = 32;
const FILE_CHANNEL_CAPACITY: usize = 16;

pub struct RsHttpServerTlsConfig {
    pub cert: String,
    pub private_key: String,
}

pub struct RsHttpServerInfo {
    pub alias: String,
    pub version: String,
    pub device_model: Option<String>,
    pub device_type: Option<localsend::model::discovery::DeviceType>,
    pub token: String,
}

pub struct RsHttpServerInternalConfig {
    pub show_token: String,
}

pub struct RsHttpServerV2Config {
    pub pin: Option<String>,
}

pub struct RsHttpServerWebSendI18n {
    pub waiting: String,
    pub enter_pin: String,
    pub invalid_pin: String,
    pub too_many_attempts: String,
    pub rejected: String,
    pub files: String,
    pub file_name: String,
    pub size: String,
}

pub struct RsHttpServerWebSendConfig {
    pub files: HashMap<String, FileDto>,
    pub pin: Option<String>,
    pub i18n: RsHttpServerWebSendI18n,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RsHttpServerSessionEndReason {
    Finished,
    Cancelled,
}

#[frb(opaque)]
pub enum RsHttpServerEvent {
    Show {
        args: Vec<String>,
    },
    Register {
        ip: String,
        info: RegisterDto,
    },
    PrepareUpload {
        ip: String,
        info: RegisterDto,
        files: HashMap<String, FileDto>,
        request: Option<RsHttpServerPrepareUploadRequest>,
    },
    FileUpload {
        session_id: String,
        file_id: String,
        file: FileDto,
        request: Option<RsHttpServerFileUploadRequest>,
    },
    SessionEnd {
        session_id: String,
        reason: RsHttpServerSessionEndReason,
    },
    PrepareDownload {
        ip: String,
        session_id: String,
        user_agent: Option<String>,
        request: Option<RsHttpServerPrepareDownloadRequest>,
    },
    FileDownload {
        session_id: String,
        file_id: String,
        file: FileDto,
        request: Option<RsHttpServerFileDownloadRequest>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RsHttpServerEventKind {
    Show,
    Register,
    PrepareUpload,
    FileUpload,
    SessionEnd,
    PrepareDownload,
    FileDownload,
}

impl RsHttpServerEvent {
    #[frb(sync)]
    pub fn kind(&self) -> RsHttpServerEventKind {
        match self {
            Self::Show { .. } => RsHttpServerEventKind::Show,
            Self::Register { .. } => RsHttpServerEventKind::Register,
            Self::PrepareUpload { .. } => RsHttpServerEventKind::PrepareUpload,
            Self::FileUpload { .. } => RsHttpServerEventKind::FileUpload,
            Self::SessionEnd { .. } => RsHttpServerEventKind::SessionEnd,
            Self::PrepareDownload { .. } => RsHttpServerEventKind::PrepareDownload,
            Self::FileDownload { .. } => RsHttpServerEventKind::FileDownload,
        }
    }

    #[frb(sync)]
    pub fn args(&self) -> Option<Vec<String>> {
        match self {
            Self::Show { args } => Some(args.clone()),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn ip(&self) -> Option<String> {
        match self {
            Self::Register { ip, .. }
            | Self::PrepareUpload { ip, .. }
            | Self::PrepareDownload { ip, .. } => Some(ip.clone()),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn info(&self) -> Option<RegisterDto> {
        match self {
            Self::Register { info, .. } | Self::PrepareUpload { info, .. } => Some(info.clone()),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn files(&self) -> Option<HashMap<String, FileDto>> {
        match self {
            Self::PrepareUpload { files, .. } => Some(files.clone()),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn session_id(&self) -> Option<String> {
        match self {
            Self::FileUpload { session_id, .. }
            | Self::SessionEnd { session_id, .. }
            | Self::PrepareDownload { session_id, .. }
            | Self::FileDownload { session_id, .. } => Some(session_id.clone()),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn file_id(&self) -> Option<String> {
        match self {
            Self::FileUpload { file_id, .. } | Self::FileDownload { file_id, .. } => {
                Some(file_id.clone())
            }
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn file(&self) -> Option<FileDto> {
        match self {
            Self::FileUpload { file, .. } | Self::FileDownload { file, .. } => Some(file.clone()),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn reason(&self) -> Option<RsHttpServerSessionEndReason> {
        match self {
            Self::SessionEnd { reason, .. } => Some(*reason),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn user_agent(&self) -> Option<String> {
        match self {
            Self::PrepareDownload { user_agent, .. } => user_agent.clone(),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn take_prepare_upload_request(&mut self) -> Option<RsHttpServerPrepareUploadRequest> {
        match self {
            Self::PrepareUpload { request, .. } => request.take(),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn take_file_upload_request(&mut self) -> Option<RsHttpServerFileUploadRequest> {
        match self {
            Self::FileUpload { request, .. } => request.take(),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn take_prepare_download_request(&mut self) -> Option<RsHttpServerPrepareDownloadRequest> {
        match self {
            Self::PrepareDownload { request, .. } => request.take(),
            _ => None,
        }
    }

    #[frb(sync)]
    pub fn take_file_download_request(&mut self) -> Option<RsHttpServerFileDownloadRequest> {
        match self {
            Self::FileDownload { request, .. } => request.take(),
            _ => None,
        }
    }
}

#[frb(opaque)]
pub struct RsHttpServer {
    event_tx: StdMutex<Option<mpsc::Sender<RsHttpServerEvent>>>,
    event_rx: Mutex<Option<mpsc::Receiver<RsHttpServerEvent>>>,
    stop_tx: StdMutex<Option<oneshot::Sender<()>>>,
}

#[frb(sync)]
pub fn create_http_server() -> RsHttpServer {
    let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    RsHttpServer {
        event_tx: StdMutex::new(Some(event_tx)),
        event_rx: Mutex::new(Some(event_rx)),
        stop_tx: StdMutex::new(None),
    }
}

impl RsHttpServer {
    pub async fn start(
        &self,
        port: u16,
        tls: Option<RsHttpServerTlsConfig>,
        info: RsHttpServerInfo,
        internal: Option<RsHttpServerInternalConfig>,
        v2: Option<RsHttpServerV2Config>,
        web_send: Option<RsHttpServerWebSendConfig>,
    ) -> anyhow::Result<()> {
        if self.stop_tx.lock().unwrap().is_some() {
            return Err(anyhow::anyhow!("HTTP server already started"));
        }

        let event_tx = self
            .event_tx
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("HTTP server already stopped"))?;

        let internal_config = internal.map(|config| {
            let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
            spawn_internal_event_forwarder(rx, event_tx.clone());
            InternalConfig {
                show_token: config.show_token,
                event_tx: tx,
            }
        });

        let v2_config = v2.map(|config| {
            let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
            spawn_v2_event_forwarder(rx, event_tx.clone());
            ServerConfigV2 {
                pin: config.pin,
                event_tx: tx,
            }
        });

        let web_send_config = web_send.map(|config| {
            let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
            spawn_web_send_event_forwarder(rx, event_tx);
            WebSendConfig {
                files: config.files,
                pin: config.pin,
                i18n: WebSendI18n {
                    waiting: config.i18n.waiting,
                    enter_pin: config.i18n.enter_pin,
                    invalid_pin: config.i18n.invalid_pin,
                    too_many_attempts: config.i18n.too_many_attempts,
                    rejected: config.i18n.rejected,
                    files: config.i18n.files,
                    file_name: config.i18n.file_name,
                    size: config.i18n.size,
                },
                event_tx: tx,
            }
        });

        let (stop_tx, stop_rx) = oneshot::channel();
        localsend::http::server::start_with_port(
            port,
            tls.map(|tls| TlsConfig {
                cert: tls.cert,
                private_key: tls.private_key,
            }),
            ClientInfo {
                alias: info.alias,
                version: info.version,
                device_model: info.device_model,
                device_type: info.device_type,
                token: info.token,
            },
            internal_config,
            v2_config,
            web_send_config,
            stop_rx,
        )
        .await?;

        *self.stop_tx.lock().unwrap() = Some(stop_tx);
        Ok(())
    }

    pub async fn listen(&self, sink: StreamSink<RsHttpServerEvent>) {
        let Some(mut event_rx) = self.event_rx.lock().await.take() else {
            let _ = sink.add_error(anyhow::anyhow!("HTTP server events already listened to"));
            return;
        };

        while let Some(event) = event_rx.recv().await {
            if sink.add(event).is_err() {
                break;
            }
        }
    }

    #[frb(sync)]
    pub fn stop(&self) {
        if let Some(stop_tx) = self.stop_tx.lock().unwrap().take() {
            let _ = stop_tx.send(());
        }
        self.event_tx.lock().unwrap().take();
    }
}

impl Drop for RsHttpServer {
    fn drop(&mut self) {
        if let Ok(stop_tx) = self.stop_tx.get_mut() {
            if let Some(stop_tx) = stop_tx.take() {
                let _ = stop_tx.send(());
            }
        }
    }
}

#[frb(opaque)]
pub struct RsHttpServerPrepareUploadRequest {
    decision_tx: Mutex<Option<oneshot::Sender<PrepareUploadDecisionV2>>>,
}

impl RsHttpServerPrepareUploadRequest {
    pub async fn accept(&self, file_ids: HashSet<String>) -> anyhow::Result<()> {
        self.respond(PrepareUploadDecisionV2::Accept(file_ids))
            .await
    }

    pub async fn decline(&self) -> anyhow::Result<()> {
        self.respond(PrepareUploadDecisionV2::Decline).await
    }

    async fn respond(&self, decision: PrepareUploadDecisionV2) -> anyhow::Result<()> {
        let decision_tx = self
            .decision_tx
            .lock()
            .await
            .take()
            .ok_or_else(|| anyhow::anyhow!("Prepare-upload request already answered"))?;
        decision_tx
            .send(decision)
            .map_err(|_| anyhow::anyhow!("Prepare-upload request closed"))
    }
}

#[frb(opaque)]
pub struct RsHttpServerFileUploadRequest {
    file_size: u64,
    target_tx: Mutex<Option<oneshot::Sender<FileUploadTarget>>>,
}

impl RsHttpServerFileUploadRequest {
    pub async fn save_to_path(&self, path: String) -> anyhow::Result<()> {
        let target_tx = self.take_target().await?;
        let (result_tx, result_rx) = oneshot::channel();
        target_tx
            .send(FileUploadTarget::Path {
                path: PathBuf::from(path),
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("File-upload request closed"))?;
        flatten_file_result(result_rx).await
    }

    pub async fn save_to_file_descriptor(&self, fd: i32) -> anyhow::Result<()> {
        #[cfg(target_os = "android")]
        {
            let target_tx = self.take_target().await?;
            let (result_tx, result_rx) = oneshot::channel();
            target_tx
                .send(FileUploadTarget::Fd { fd, result_tx })
                .map_err(|_| anyhow::anyhow!("File-upload request closed"))?;
            return flatten_file_result(result_rx).await;
        }
        #[cfg(not(target_os = "android"))]
        {
            let _ = fd;
            Err(anyhow::anyhow!(
                "File descriptors are only supported on Android"
            ))
        }
    }

    pub async fn receive(&self, sink: StreamSink<Vec<u8>>) -> anyhow::Result<()> {
        let target_tx = self.take_target().await?;
        let (binary_tx, mut binary_rx) = mpsc::channel::<Bytes>(FILE_CHANNEL_CAPACITY);
        let (result_tx, result_rx) = oneshot::channel::<Result<(), String>>();
        target_tx
            .send(FileUploadTarget::Stream {
                binary_tx,
                result_rx,
            })
            .map_err(|_| anyhow::anyhow!("File-upload request closed"))?;

        let mut received = 0_u64;
        let result = 'receive: {
            while let Some(chunk) = binary_rx.recv().await {
                received += chunk.len() as u64;
                if received > self.file_size {
                    break 'receive Err(format!(
                        "Expected {} bytes, received at least {received}",
                        self.file_size
                    ));
                }
                if sink.add(chunk.to_vec()).is_err() {
                    break 'receive Err("File-upload stream listener closed".to_string());
                }
            }
            Ok(())
        };

        let result = result.and_then(|()| {
            if received == self.file_size {
                Ok(())
            } else {
                Err(format!(
                    "Expected {} bytes, received {received}",
                    self.file_size
                ))
            }
        });
        let _ = result_tx.send(result.clone());
        result.map_err(anyhow::Error::msg)
    }

    async fn take_target(&self) -> anyhow::Result<oneshot::Sender<FileUploadTarget>> {
        self.target_tx
            .lock()
            .await
            .take()
            .ok_or_else(|| anyhow::anyhow!("File-upload request already answered"))
    }
}

#[frb(opaque)]
pub struct RsHttpServerPrepareDownloadRequest {
    decision_tx: Mutex<Option<oneshot::Sender<bool>>>,
}

impl RsHttpServerPrepareDownloadRequest {
    pub async fn accept(&self) -> anyhow::Result<()> {
        self.respond(true).await
    }

    pub async fn decline(&self) -> anyhow::Result<()> {
        self.respond(false).await
    }

    async fn respond(&self, accepted: bool) -> anyhow::Result<()> {
        let decision_tx = self
            .decision_tx
            .lock()
            .await
            .take()
            .ok_or_else(|| anyhow::anyhow!("Prepare-download request already answered"))?;
        decision_tx
            .send(accepted)
            .map_err(|_| anyhow::anyhow!("Prepare-download request closed"))
    }
}

#[frb(opaque)]
pub struct RsHttpServerFileDownloadRequest {
    content_tx: Mutex<Option<oneshot::Sender<FileContent>>>,
}

impl RsHttpServerFileDownloadRequest {
    pub async fn provide_path(&self, path: String) -> anyhow::Result<()> {
        self.send_content(FileContent::Path(PathBuf::from(path)))
            .await
    }

    pub async fn provide_file_descriptor(&self, fd: i32) -> anyhow::Result<()> {
        #[cfg(target_os = "android")]
        {
            return self.send_content(FileContent::Fd(fd)).await;
        }
        #[cfg(not(target_os = "android"))]
        {
            let _ = fd;
            Err(anyhow::anyhow!(
                "File descriptors are only supported on Android"
            ))
        }
    }

    pub async fn provide_bytes(&self, data: Vec<u8>) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel(1);
        tx.send(Bytes::from(data))
            .await
            .map_err(|_| anyhow::anyhow!("Failed to buffer download content"))?;
        drop(tx);
        self.send_content(FileContent::Stream(rx)).await
    }

    pub async fn provide_stream(&self, stream: Dart2RustStreamReceiver) -> anyhow::Result<()> {
        self.send_content(FileContent::Stream(stream.receiver))
            .await
    }

    async fn send_content(&self, content: FileContent) -> anyhow::Result<()> {
        let content_tx = self
            .content_tx
            .lock()
            .await
            .take()
            .ok_or_else(|| anyhow::anyhow!("File-download request already answered"))?;
        content_tx
            .send(content)
            .map_err(|_| anyhow::anyhow!("File-download request closed"))
    }
}

async fn flatten_file_result(
    result_rx: oneshot::Receiver<Result<(), String>>,
) -> anyhow::Result<()> {
    result_rx
        .await
        .map_err(|_| anyhow::anyhow!("File-upload result channel closed"))?
        .map_err(anyhow::Error::msg)
}

fn spawn_internal_event_forwarder(
    mut rx: mpsc::Receiver<InternalEvent>,
    event_tx: mpsc::Sender<RsHttpServerEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let event = match event {
                InternalEvent::Show { args } => RsHttpServerEvent::Show { args },
            };
            if event_tx.send(event).await.is_err() {
                break;
            }
        }
    });
}

fn spawn_v2_event_forwarder(
    mut rx: mpsc::Receiver<ServerEventV2>,
    event_tx: mpsc::Sender<RsHttpServerEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let event = match event {
                ServerEventV2::Register { ip, info } => RsHttpServerEvent::Register {
                    ip: ip.to_string(),
                    info: register_dto_from_v2(info),
                },
                ServerEventV2::PrepareUpload {
                    ip,
                    info,
                    files,
                    decision_tx,
                } => RsHttpServerEvent::PrepareUpload {
                    ip: ip.to_string(),
                    info: register_dto_from_v2(info),
                    files,
                    request: Some(RsHttpServerPrepareUploadRequest {
                        decision_tx: Mutex::new(Some(decision_tx)),
                    }),
                },
                ServerEventV2::FileUpload {
                    session_id,
                    file_id,
                    file,
                    target_tx,
                } => {
                    let file_size = file.size;
                    RsHttpServerEvent::FileUpload {
                        session_id,
                        file_id,
                        file,
                        request: Some(RsHttpServerFileUploadRequest {
                            file_size,
                            target_tx: Mutex::new(Some(target_tx)),
                        }),
                    }
                }
                ServerEventV2::SessionEnd { session_id, reason } => RsHttpServerEvent::SessionEnd {
                    session_id,
                    reason: match reason {
                        SessionEndReasonV2::Finished => RsHttpServerSessionEndReason::Finished,
                        SessionEndReasonV2::Cancelled => RsHttpServerSessionEndReason::Cancelled,
                    },
                },
            };
            if event_tx.send(event).await.is_err() {
                break;
            }
        }
    });
}

fn spawn_web_send_event_forwarder(
    mut rx: mpsc::Receiver<WebSendEvent>,
    event_tx: mpsc::Sender<RsHttpServerEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let event = match event {
                WebSendEvent::PrepareDownload {
                    ip,
                    session_id,
                    user_agent,
                    decision_tx,
                } => RsHttpServerEvent::PrepareDownload {
                    ip: ip.to_string(),
                    session_id,
                    user_agent,
                    request: Some(RsHttpServerPrepareDownloadRequest {
                        decision_tx: Mutex::new(Some(decision_tx)),
                    }),
                },
                WebSendEvent::FileDownload {
                    session_id,
                    file_id,
                    file,
                    content_tx,
                } => RsHttpServerEvent::FileDownload {
                    session_id,
                    file_id,
                    file,
                    request: Some(RsHttpServerFileDownloadRequest {
                        content_tx: Mutex::new(Some(content_tx)),
                    }),
                },
            };
            if event_tx.send(event).await.is_err() {
                break;
            }
        }
    });
}

fn register_dto_from_v2(info: RegisterDtoV2) -> RegisterDto {
    RegisterDto {
        alias: info.alias,
        version: info.version,
        device_model: info.device_model,
        device_type: info.device_type,
        token: info.fingerprint,
        port: info.port,
        protocol: match info.protocol {
            ProtocolTypeV2::Http => ProtocolType::Http,
            ProtocolTypeV2::Https => ProtocolType::Https,
        },
        has_web_interface: info.download,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn starts_and_stops_server() {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let server = create_http_server();
        server
            .start(
                port,
                None,
                RsHttpServerInfo {
                    alias: "test".to_string(),
                    version: "2.1".to_string(),
                    device_model: None,
                    device_type: None,
                    token: "test-token".to_string(),
                },
                None,
                Some(RsHttpServerV2Config { pin: None }),
                None,
            )
            .await
            .unwrap();
        server.stop();
    }
}
