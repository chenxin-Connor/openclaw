package ai.openclaw.app.node

import android.Manifest
import android.content.Context
import android.media.MediaRecorder
import android.os.ParcelFileDescriptor
import androidx.core.content.ContextCompat
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
@Suppress("DEPRECATION")

class AudioCaptureManager(private val context: Context) {

  fun ensureMicPermission() {
    if (ContextCompat.checkSelfPermission(context, Manifest.permission.RECORD_AUDIO) !=
        android.content.pm.PackageManager.PERMISSION_GRANTED
    ) {
      throw IllegalStateException("PERMISSION_DENIED: microphone permission not granted")
    }
  }

  suspend fun record(paramsJson: String?): AudioPayload = withContext(Dispatchers.IO) {
    ensureMicPermission()

    val params = parseJsonParamsObject(paramsJson)
    val durationMs = parseDurationMs(params)?.coerceIn(100, 300_000) ?: 10_000

    val file = File.createTempFile("openclaw-audio-", ".m4a")

    val recorder = MediaRecorder().apply {
      setAudioSource(MediaRecorder.AudioSource.MIC)
      setOutputFormat(MediaRecorder.OutputFormat.MPEG_4)
      setAudioEncoder(MediaRecorder.AudioEncoder.AAC)
      setAudioSamplingRate(44100)
      setAudioEncodingBitRate(128_000)
      setOutputFile(file.absolutePath)
    }

    try {
      recorder.prepare()
      recorder.start()
      Thread.sleep(durationMs.toLong())
      recorder.stop()
    } catch (err: Throwable) {
      recorder.release()
      file.delete()
      throw err
    }

    recorder.release()

    val bytes = file.readBytes()
    file.delete()

    AudioPayload(
      format = "m4a",
      bytes = bytes,
      durationMs = durationMs,
    )
  }

  private fun parseJsonParamsObject(raw: String?): kotlinx.serialization.json.JsonObject? {
    if (raw.isNullOrBlank()) return null
    return try {
      kotlinx.serialization.json.Json.parseToJsonElement(raw).asObjectOrNull()
    } catch (_: Throwable) {
      null
    }
  }

  private fun parseDurationMs(params: kotlinx.serialization.json.JsonObject?): Int? {
    val prim = params?.get("durationMs") as? kotlinx.serialization.json.JsonPrimitive ?: return null
    val value = prim.content.trim().toIntOrNull()
    return value?.takeIf { it > 0 }
  }

  private fun kotlinx.serialization.json.JsonElement.asObjectOrNull(): kotlinx.serialization.json.JsonObject? =
    this as? kotlinx.serialization.json.JsonObject
}

data class AudioPayload(
  val format: String,
  val bytes: ByteArray,
  val durationMs: Int,
)
