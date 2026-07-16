import 'dart:async';
import 'dart:typed_data';

import 'package:localsend_app/isolate/constants.dart';
import 'package:localsend_app/isolate/model/device.dart' as app;
import 'package:localsend_app/isolate/model/dto/file_dto.dart' as app;
import 'package:localsend_app/isolate/model/dto/multicast_dto.dart' as app;
import 'package:localsend_app/isolate/model/dto/register_dto.dart' as app;
import 'package:localsend_app/isolate/model/stored_security_context.dart';
import 'package:localsend_app/isolate/src/isolate/child/http_server_events.dart';
import 'package:localsend_app/isolate/src/isolate/child/main.dart';
import 'package:localsend_app/isolate/src/isolate/dto/send_to_isolate_data.dart';
import 'package:localsend_app/rust/api/http_server.dart' as rust;
import 'package:localsend_app/rust/api/model.dart' as rust;
import 'package:localsend_app/rust/api/stream.dart';

sealed class HttpServerTask {
  const HttpServerTask();
}

class HttpServerStartTask extends HttpServerTask {
  final String alias;
  final int port;
  final bool https;
  final String? deviceModel;
  final app.DeviceType deviceType;
  final String fingerprint;
  final StoredSecurityContext securityContext;
  final String? showToken;
  final String? receivePin;
  final HttpServerWebSendConfig? webSend;

  const HttpServerStartTask({
    required this.alias,
    required this.port,
    required this.https,
    required this.deviceModel,
    required this.deviceType,
    required this.fingerprint,
    required this.securityContext,
    required this.showToken,
    required this.receivePin,
    required this.webSend,
  });
}

class HttpServerWebSendConfig {
  final Map<String, app.FileDto> files;
  final String? pin;
  final HttpServerWebSendI18n i18n;

  const HttpServerWebSendConfig({
    required this.files,
    required this.pin,
    required this.i18n,
  });
}

class HttpServerWebSendI18n {
  final String waiting;
  final String enterPin;
  final String invalidPin;
  final String tooManyAttempts;
  final String rejected;
  final String files;
  final String fileName;
  final String size;

  const HttpServerWebSendI18n({
    required this.waiting,
    required this.enterPin,
    required this.invalidPin,
    required this.tooManyAttempts,
    required this.rejected,
    required this.files,
    required this.fileName,
    required this.size,
  });
}

class HttpServerStopTask extends HttpServerTask {
  const HttpServerStopTask();
}

class HttpServerRespondPrepareUploadTask extends HttpServerTask {
  final int requestId;
  final Set<String>? fileIds;

  const HttpServerRespondPrepareUploadTask({
    required this.requestId,
    required this.fileIds,
  });
}

sealed class HttpServerFileUploadTarget {
  const HttpServerFileUploadTarget();
}

class HttpServerFileUploadPathTarget extends HttpServerFileUploadTarget {
  final String path;

  const HttpServerFileUploadPathTarget({required this.path});
}

class HttpServerFileUploadDescriptorTarget extends HttpServerFileUploadTarget {
  final int fileDescriptor;

  const HttpServerFileUploadDescriptorTarget({required this.fileDescriptor});
}

class HttpServerFileUploadStreamTarget extends HttpServerFileUploadTarget {
  const HttpServerFileUploadStreamTarget();
}

class HttpServerSetFileUploadTargetTask extends HttpServerTask {
  final int requestId;
  final HttpServerFileUploadTarget target;

  const HttpServerSetFileUploadTargetTask({
    required this.requestId,
    required this.target,
  });
}

class HttpServerRespondPrepareDownloadTask extends HttpServerTask {
  final int requestId;
  final bool accepted;

  const HttpServerRespondPrepareDownloadTask({
    required this.requestId,
    required this.accepted,
  });
}

sealed class HttpServerFileDownloadContent {
  const HttpServerFileDownloadContent();
}

class HttpServerFileDownloadPathContent extends HttpServerFileDownloadContent {
  final String path;

  const HttpServerFileDownloadPathContent({required this.path});
}

class HttpServerFileDownloadDescriptorContent extends HttpServerFileDownloadContent {
  final int fileDescriptor;

  const HttpServerFileDownloadDescriptorContent({required this.fileDescriptor});
}

class HttpServerFileDownloadBytesContent extends HttpServerFileDownloadContent {
  final Uint8List bytes;

  const HttpServerFileDownloadBytesContent({required this.bytes});
}

class HttpServerFileDownloadStreamContent extends HttpServerFileDownloadContent {
  final Stream<List<int>> stream;

  const HttpServerFileDownloadStreamContent({required this.stream});
}

class HttpServerSetFileDownloadContentTask extends HttpServerTask {
  final int requestId;
  final HttpServerFileDownloadContent content;

  const HttpServerSetFileDownloadContentTask({
    required this.requestId,
    required this.content,
  });
}

Future<void> setupHttpServerIsolate(
  Stream<SendToIsolateData<HttpServerTask>> receiveFromMain,
  void Function(HttpServerEvent) sendToMain,
  InitialData initialData,
) async {
  final bindings = _HttpServerBindings(sendToMain);
  await setupChildIsolateHelper(
    debugLabel: 'HttpServerIsolate',
    receiveFromMain: receiveFromMain,
    sendToMain: sendToMain,
    initialData: initialData,
    handler: (_, task) => bindings.handle(task),
  );
}

class _HttpServerBindings {
  final void Function(HttpServerEvent) sendToMain;
  final Map<int, rust.RsHttpServerPrepareUploadRequest> _prepareUploads = {};
  final Map<int, rust.RsHttpServerFileUploadRequest> _fileUploads = {};
  final Map<int, rust.RsHttpServerPrepareDownloadRequest> _prepareDownloads = {};
  final Map<int, rust.RsHttpServerFileDownloadRequest> _fileDownloads = {};

  rust.RsHttpServer? _server;
  StreamSubscription<rust.RsHttpServerEvent>? _eventSubscription;
  Future<void>? _startOperation;
  int _nextRequestId = 0;

  _HttpServerBindings(this.sendToMain);

  Future<void> handle(HttpServerTask task) async {
    try {
      switch (task) {
        case HttpServerStartTask():
          if (_startOperation != null || _server != null) {
            sendToMain(const HttpServerErrorEvent(error: 'HTTP server already started'));
            return;
          }
          final operation = _start(task);
          _startOperation = operation;
          try {
            await operation;
          } finally {
            if (identical(_startOperation, operation)) {
              _startOperation = null;
            }
          }
        case HttpServerStopTask():
          await _startOperation;
          await _stop();
        case HttpServerRespondPrepareUploadTask():
          await _respondPrepareUpload(task);
        case HttpServerSetFileUploadTargetTask():
          await _setFileUploadTarget(task);
        case HttpServerRespondPrepareDownloadTask():
          await _respondPrepareDownload(task);
        case HttpServerSetFileDownloadContentTask():
          await _setFileDownloadContent(task);
      }
    } catch (error) {
      sendToMain(HttpServerErrorEvent(error: error.toString()));
    }
  }

  Future<void> _start(HttpServerStartTask task) async {
    final server = rust.createHttpServer();
    _server = server;
    _eventSubscription = server.listen().listen(
      _handleEvent,
      onError: (Object error, StackTrace stackTrace) {
        sendToMain(HttpServerErrorEvent(error: error.toString()));
      },
    );

    try {
      await server.start(
        port: task.port,
        tls: task.https
            ? rust.RsHttpServerTlsConfig(
                cert: task.securityContext.certificate,
                privateKey: task.securityContext.privateKey,
              )
            : null,
        info: rust.RsHttpServerInfo(
          alias: task.alias,
          version: protocolVersion,
          deviceModel: task.deviceModel,
          deviceType: rust.DeviceType.values.byName(task.deviceType.name),
          token: task.fingerprint,
        ),
        internal: task.showToken == null ? null : rust.RsHttpServerInternalConfig(showToken: task.showToken!),
        v2: rust.RsHttpServerV2Config(pin: task.receivePin),
        webSend: task.webSend?._toRust(),
      );
      sendToMain(const HttpServerStartedEvent());
    } catch (error) {
      await _disposeServer();
      sendToMain(HttpServerErrorEvent(error: error.toString()));
    }
  }

  Future<void> _stop() async {
    if (_server == null) {
      return;
    }
    await _disposeServer();
    sendToMain(const HttpServerStoppedEvent());
  }

  Future<void> _disposeServer() async {
    _server?.stop();
    _server = null;
    await _eventSubscription?.cancel();
    _eventSubscription = null;
    _prepareUploads.clear();
    _fileUploads.clear();
    _prepareDownloads.clear();
    _fileDownloads.clear();
  }

  void _handleEvent(rust.RsHttpServerEvent event) {
    switch (event.kind()) {
      case rust.RsHttpServerEventKind.show_:
        sendToMain(HttpServerShowEvent(args: event.args()!));
      case rust.RsHttpServerEventKind.register:
        sendToMain(HttpServerRegisterEvent(ip: event.ip()!, info: event.info()!._toApp()));
      case rust.RsHttpServerEventKind.prepareUpload:
        final requestId = _newRequestId();
        _prepareUploads[requestId] = event.takePrepareUploadRequest()!;
        sendToMain(
          HttpServerPrepareUploadEvent(
            requestId: requestId,
            ip: event.ip()!,
            info: event.info()!._toApp(),
            files: event.files()!.map((id, file) => MapEntry(id, file._toApp())),
          ),
        );
      case rust.RsHttpServerEventKind.fileUpload:
        final requestId = _newRequestId();
        _fileUploads[requestId] = event.takeFileUploadRequest()!;
        sendToMain(
          HttpServerFileUploadEvent(
            requestId: requestId,
            sessionId: event.sessionId()!,
            fileId: event.fileId()!,
            file: event.file()!._toApp(),
          ),
        );
      case rust.RsHttpServerEventKind.sessionEnd:
        sendToMain(
          HttpServerSessionEndEvent(
            sessionId: event.sessionId()!,
            reason: HttpServerSessionEndReason.values.byName(event.reason()!.name),
          ),
        );
      case rust.RsHttpServerEventKind.prepareDownload:
        final requestId = _newRequestId();
        _prepareDownloads[requestId] = event.takePrepareDownloadRequest()!;
        sendToMain(
          HttpServerPrepareDownloadEvent(
            requestId: requestId,
            ip: event.ip()!,
            sessionId: event.sessionId()!,
            userAgent: event.userAgent(),
          ),
        );
      case rust.RsHttpServerEventKind.fileDownload:
        final requestId = _newRequestId();
        _fileDownloads[requestId] = event.takeFileDownloadRequest()!;
        sendToMain(
          HttpServerFileDownloadEvent(
            requestId: requestId,
            sessionId: event.sessionId()!,
            fileId: event.fileId()!,
            file: event.file()!._toApp(),
          ),
        );
    }
  }

  int _newRequestId() => _nextRequestId++;

  Future<void> _respondPrepareUpload(HttpServerRespondPrepareUploadTask task) async {
    final request = _prepareUploads.remove(task.requestId);
    if (request == null) {
      return;
    }
    final fileIds = task.fileIds;
    if (fileIds == null) {
      await request.decline();
    } else {
      await request.accept(fileIds: fileIds);
    }
  }

  Future<void> _respondPrepareDownload(HttpServerRespondPrepareDownloadTask task) async {
    final request = _prepareDownloads.remove(task.requestId);
    if (request == null) {
      return;
    }
    if (task.accepted) {
      await request.accept();
    } else {
      await request.decline();
    }
  }

  Future<void> _setFileUploadTarget(HttpServerSetFileUploadTargetTask task) async {
    final request = _fileUploads.remove(task.requestId);
    if (request == null) {
      return;
    }

    switch (task.target) {
      case HttpServerFileUploadPathTarget(:final path):
        await _reportFileUploadResult(task.requestId, request.saveToPath(path: path));
      case HttpServerFileUploadDescriptorTarget(:final fileDescriptor):
        await _reportFileUploadResult(task.requestId, request.saveToFileDescriptor(fd: fileDescriptor));
      case HttpServerFileUploadStreamTarget():
        var failed = false;
        request.receive().listen(
          (data) => sendToMain(HttpServerFileUploadChunkEvent(requestId: task.requestId, data: data)),
          onError: (Object error, StackTrace stackTrace) {
            failed = true;
            sendToMain(HttpServerFileUploadFinishedEvent(requestId: task.requestId, error: error.toString()));
          },
          onDone: () {
            if (!failed) {
              sendToMain(HttpServerFileUploadFinishedEvent(requestId: task.requestId, error: null));
            }
          },
        );
    }
  }

  Future<void> _reportFileUploadResult(int requestId, Future<void> result) async {
    try {
      await result;
      sendToMain(HttpServerFileUploadFinishedEvent(requestId: requestId, error: null));
    } catch (error) {
      sendToMain(HttpServerFileUploadFinishedEvent(requestId: requestId, error: error.toString()));
    }
  }

  Future<void> _setFileDownloadContent(HttpServerSetFileDownloadContentTask task) async {
    final request = _fileDownloads.remove(task.requestId);
    if (request == null) {
      return;
    }

    switch (task.content) {
      case HttpServerFileDownloadPathContent(:final path):
        await request.providePath(path: path);
      case HttpServerFileDownloadDescriptorContent(:final fileDescriptor):
        await request.provideFileDescriptor(fd: fileDescriptor);
      case HttpServerFileDownloadBytesContent(:final bytes):
        await request.provideBytes(data: bytes);
      case HttpServerFileDownloadStreamContent(:final stream):
        final (sink, receiver) = await createStream();
        await request.provideStream(stream: receiver);
        unawaited(
          _pipeStream(stream, sink).catchError((Object error) {
            sink.close();
          }),
        );
    }
  }
}

Future<void> _pipeStream(Stream<List<int>> stream, Dart2RustStreamSink sink) async {
  try {
    await for (final data in stream) {
      await sink.add(data: data);
    }
  } finally {
    sink.close();
  }
}

extension on HttpServerWebSendConfig {
  rust.RsHttpServerWebSendConfig _toRust() => rust.RsHttpServerWebSendConfig(
    files: files.map((id, file) => MapEntry(id, file._toRust())),
    pin: pin,
    i18N: rust.RsHttpServerWebSendI18n(
      waiting: i18n.waiting,
      enterPin: i18n.enterPin,
      invalidPin: i18n.invalidPin,
      tooManyAttempts: i18n.tooManyAttempts,
      rejected: i18n.rejected,
      files: i18n.files,
      fileName: i18n.fileName,
      size: i18n.size,
    ),
  );
}

extension on rust.RegisterDto {
  app.RegisterDto _toApp() => app.RegisterDto(
    alias: alias,
    version: version,
    deviceModel: deviceModel,
    deviceType: deviceType == null ? null : app.DeviceType.values.byName(deviceType!.name),
    fingerprint: token,
    port: port,
    protocol: app.ProtocolType.values.byName(protocol.name),
    download: hasWebInterface,
  );
}

extension on rust.FileDto {
  app.FileDto _toApp() => app.FileDto(
    id: id,
    fileName: fileName,
    size: size.toInt(),
    fileType: app.decodeFromMime(fileType),
    hash: sha256,
    preview: preview,
    metadata: metadata == null
        ? null
        : app.FileMetadata(
            lastModified: metadata!.modified == null ? null : DateTime.tryParse(metadata!.modified!),
            lastAccessed: metadata!.accessed == null ? null : DateTime.tryParse(metadata!.accessed!),
          ),
  );
}

extension on app.FileDto {
  rust.FileDto _toRust() => rust.FileDto(
    id: id,
    fileName: fileName,
    size: BigInt.from(size),
    fileType: lookupMime(),
    sha256: hash,
    preview: preview,
    metadata: metadata == null
        ? null
        : rust.FileMetadata(
            modified: metadata!.lastModified?.toIso8601String(),
            accessed: metadata!.lastAccessed?.toIso8601String(),
          ),
  );
}
