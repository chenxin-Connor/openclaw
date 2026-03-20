package ai.openclaw.app.node

import ai.openclaw.app.gateway.GatewaySession

class AudioHandler(
  private val audio: AudioCaptureManager,
  private val invokeErrorFromThrowable: (err: Throwable) -> Pair<String, String>,
) {
  suspend fun handleRecord(paramsJson: String?): GatewaySession.InvokeResult {
    return try {
      val result = audio.record(paramsJson)
      val base64 = android.util.Base64.encodeToString(result.bytes, android.util.Base64.NO_WRAP)
      GatewaySession.InvokeResult.ok(
        """{"format":"${result.format}","base64":"$base64","durationMs":${result.durationMs},"size":${result.bytes.size}}"""
      )
    } catch (err: Throwable) {
      val (code, message) = invokeErrorFromThrowable(err)
      GatewaySession.InvokeResult.error(code = code, message = message)
    }
  }
}
