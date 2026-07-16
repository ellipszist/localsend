import 'dart:typed_data';

import 'package:localsend_app/isolate/model/dto/file_dto.dart';
import 'package:localsend_app/isolate/model/dto/register_dto.dart';

sealed class HttpServerEvent {
  const HttpServerEvent();
}

class HttpServerStartedEvent extends HttpServerEvent {
  const HttpServerStartedEvent();
}

class HttpServerStoppedEvent extends HttpServerEvent {
  const HttpServerStoppedEvent();
}

class HttpServerErrorEvent extends HttpServerEvent {
  final String error;

  const HttpServerErrorEvent({required this.error});
}

class HttpServerShowEvent extends HttpServerEvent {
  final List<String> args;

  const HttpServerShowEvent({required this.args});
}

class HttpServerRegisterEvent extends HttpServerEvent {
  final String ip;
  final RegisterDto info;

  const HttpServerRegisterEvent({required this.ip, required this.info});
}

class HttpServerPrepareUploadEvent extends HttpServerEvent {
  final int requestId;
  final String ip;
  final RegisterDto info;
  final Map<String, FileDto> files;

  const HttpServerPrepareUploadEvent({
    required this.requestId,
    required this.ip,
    required this.info,
    required this.files,
  });
}

class HttpServerFileUploadEvent extends HttpServerEvent {
  final int requestId;
  final String sessionId;
  final String fileId;
  final FileDto file;

  const HttpServerFileUploadEvent({
    required this.requestId,
    required this.sessionId,
    required this.fileId,
    required this.file,
  });
}

class HttpServerFileUploadChunkEvent extends HttpServerEvent {
  final int requestId;
  final Uint8List data;

  const HttpServerFileUploadChunkEvent({
    required this.requestId,
    required this.data,
  });
}

class HttpServerFileUploadFinishedEvent extends HttpServerEvent {
  final int requestId;
  final String? error;

  const HttpServerFileUploadFinishedEvent({
    required this.requestId,
    required this.error,
  });
}

enum HttpServerSessionEndReason { finished, cancelled }

class HttpServerSessionEndEvent extends HttpServerEvent {
  final String sessionId;
  final HttpServerSessionEndReason reason;

  const HttpServerSessionEndEvent({
    required this.sessionId,
    required this.reason,
  });
}

class HttpServerPrepareDownloadEvent extends HttpServerEvent {
  final int requestId;
  final String ip;
  final String sessionId;
  final String? userAgent;

  const HttpServerPrepareDownloadEvent({
    required this.requestId,
    required this.ip,
    required this.sessionId,
    required this.userAgent,
  });
}

class HttpServerFileDownloadEvent extends HttpServerEvent {
  final int requestId;
  final String sessionId;
  final String fileId;
  final FileDto file;

  const HttpServerFileDownloadEvent({
    required this.requestId,
    required this.sessionId,
    required this.fileId,
    required this.file,
  });
}
