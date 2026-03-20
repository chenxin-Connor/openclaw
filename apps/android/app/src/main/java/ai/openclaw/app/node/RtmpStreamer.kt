package ai.openclaw.app.node

import android.Manifest
import android.content.Context
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaCodec
import android.media.MediaCodecInfo
import android.media.MediaFormat
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.util.Log
import android.view.Surface
import androidx.camera.core.CameraSelector
import androidx.camera.core.Preview
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.core.content.ContextCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import ai.openclaw.app.node.rtmp.RtmpClient
import androidx.camera.core.SurfaceRequest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * RTMP streamer using CameraX + MediaCodec + pure Java RTMP client.
 *
 * Architecture:
 *   CameraX (Surface) -> MediaCodec H264 -> RtmpClient -> RTMP server
 *   AudioRecord (PCM)  -> MediaCodec AAC  -> RtmpClient -> RTMP server
 */
@Suppress("TooManyFunctions")
class RtmpStreamer(private val context: Context) {

  @Volatile private var isStreaming = false
  @Volatile private var stopRequested = false

  private var videoEncoder: MediaCodec? = null
  private var audioEncoder: MediaCodec? = null
  private var audioRecord: AudioRecord? = null
  private var videoThread: Thread? = null
  private var audioThread: Thread? = null
  private var cameraProvider: ProcessCameraProvider? = null
  private var rtmpClient: RtmpClient? = null

  private val mainHandler = Handler(Looper.getMainLooper())

  private val rtmpListener = object : RtmpClient.Listener {
    override fun onConnected() {
      Log.w("RtmpStreamer", "RTMP connected")
    }
    override fun onConnectionFailed(reason: String) {
      Log.w("RtmpStreamer", "RTMP connection failed: $reason")
      synchronized(this@RtmpStreamer) { stopRequested = true }
    }
    override fun onDisconnected() {
      Log.w("RtmpStreamer", "RTMP disconnected")
      synchronized(this@RtmpStreamer) { stopRequested = true }
    }
  }

  fun ensurePermissions(includeAudio: Boolean) {
    if (ContextCompat.checkSelfPermission(context, Manifest.permission.CAMERA) !=
        android.content.pm.PackageManager.PERMISSION_GRANTED
    ) {
      throw IllegalStateException("PERMISSION_DENIED: camera permission not granted")
    }
    if (includeAudio &&
      ContextCompat.checkSelfPermission(context, Manifest.permission.RECORD_AUDIO) !=
        android.content.pm.PackageManager.PERMISSION_GRANTED
    ) {
      throw IllegalStateException("PERMISSION_DENIED: microphone permission not granted")
    }
  }

  /**
   * Start RTMP streaming. CameraX operations are dispatched to the main thread.
   * This method can be called from any thread.
   */
  fun start(
    rtmpUrl: String,
    facing: String = "back",
    width: Int = 1280,
    height: Int = 720,
    videoBitrate: Int = 2_000_000,
    audioBitrate: Int = 128_000,
    fps: Int = 30,
    includeAudio: Boolean = true,
  ) {
    synchronized(this) {
      if (isStreaming) throw IllegalStateException("ALREADY_STREAMING: rtmp stream is already active")
      isStreaming = true
      stopRequested = false
    }

    // Step 1: Connect RTMP (can be on any thread)
    val client = RtmpClient(rtmpListener)
    rtmpClient = client
    client.connect(rtmpUrl)

    Thread.sleep(500)
    if (!client.isConnected() && !client.isStreaming()) {
      synchronized(this) { isStreaming = false }
      throw IllegalStateException("RTMP_CONNECT_FAILED: could not connect to $rtmpUrl")
    }

    // Step 2: Start camera on main thread (required by CameraX)
    val error = AtomicReference<Throwable?>(null)
    val latch = CountDownLatch(1)

    mainHandler.post {
      try {
        startCamera(client, facing, width, height, videoBitrate, fps)
        if (includeAudio) startAudio(client, audioBitrate)
      } catch (e: Throwable) {
        error.set(e)
        synchronized(this@RtmpStreamer) { isStreaming = false; stopRequested = true }
        try { client.close() } catch (_: Exception) {}
        rtmpClient = null
      } finally {
        latch.countDown()
      }
    }

    latch.await(15, TimeUnit.SECONDS)

    error.get()?.let { throw it }
  }

  private fun startCamera(
    client: RtmpClient,
    facing: String,
    width: Int,
    height: Int,
    bitrate: Int,
    fps: Int,
  ) {
    val providerFuture = ProcessCameraProvider.getInstance(context)
    val provider = providerFuture.get()
    cameraProvider = provider

    // Create H264 encoder
    val videoFormat = MediaFormat.createVideoFormat(
      MediaFormat.MIMETYPE_VIDEO_AVC, width, height,
    ).apply {
      setInteger(MediaFormat.KEY_COLOR_FORMAT, 2135033992) // COLOR_FormatSurface
      setInteger(MediaFormat.KEY_BIT_RATE, bitrate)
      setInteger(MediaFormat.KEY_FRAME_RATE, fps)
      setInteger(MediaFormat.KEY_I_FRAME_INTERVAL, 2)
      if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
        setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, width * height)
      }
    }

    val encoder = MediaCodec.createEncoderByType(MediaFormat.MIMETYPE_VIDEO_AVC)
    encoder.configure(videoFormat, null, null, MediaCodec.CONFIGURE_FLAG_ENCODE)
    val surface = encoder.createInputSurface()
    videoEncoder = encoder
    encoder.start()

    // Video encoding thread — reads encoder output and sends to RTMP
    videoThread = Thread({
      val bufferInfo = MediaCodec.BufferInfo()
      while (!stopRequested) {
        val outputBufferIndex = encoder.dequeueOutputBuffer(bufferInfo, 10000)
        when {
          outputBufferIndex >= 0 -> {
            val outputBuffer = encoder.getOutputBuffer(outputBufferIndex)
            if (outputBuffer != null && bufferInfo.size > 0) {
              val data = ByteArray(bufferInfo.size)
              outputBuffer.position(bufferInfo.offset)
              outputBuffer.get(data)
              client.publishVideoData(data, bufferInfo.presentationTimeUs, bufferInfo.flags)
            }
            encoder.releaseOutputBuffer(outputBufferIndex, false)
          }
          outputBufferIndex == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED -> {
            Log.w("RtmpStreamer", "Video format changed")
          }
        }
      }
      try { encoder.stop() } catch (_: Exception) {}
      try { encoder.release() } catch (_: Exception) {}
    }, "rtmp-video").also { it.start() }

    // CameraX Preview — provides frames to encoder surface
    val preview = Preview.Builder().build()
    val executor = ContextCompat.getMainExecutor(context)
    val noOpCallback = androidx.core.util.Consumer<SurfaceRequest.Result> { }
    preview.setSurfaceProvider { request ->
      request.provideSurface(surface, executor, noOpCallback)
    }

    val selector = when (facing.lowercase()) {
      "front" -> CameraSelector.DEFAULT_FRONT_CAMERA
      else -> CameraSelector.DEFAULT_BACK_CAMERA
    }

    try {
      provider.unbindAll()
      val lifecycleOwner = SimpleLifecycleOwner()
      provider.bindToLifecycle(lifecycleOwner, selector, preview)
      Log.i("RtmpStreamer", "CameraX bound: ${facing} ${width}x${height}")
    } catch (e: Exception) {
      throw IllegalStateException("CAMERA_ERROR: ${e.message}")
    }
  }

  private fun startAudio(client: RtmpClient, bitrate: Int) {
    val sampleRate = 44100
    val channelConfig = AudioFormat.CHANNEL_IN_MONO
    val audioFormat = AudioFormat.ENCODING_PCM_16BIT
    val minBufSize = AudioRecord.getMinBufferSize(sampleRate, channelConfig, audioFormat)

    val audioMediaFormat = MediaFormat.createAudioFormat(
      MediaFormat.MIMETYPE_AUDIO_AAC, sampleRate, 1,
    ).apply {
      setInteger(MediaFormat.KEY_BIT_RATE, bitrate)
      setInteger(MediaFormat.KEY_AAC_PROFILE, MediaCodecInfo.CodecProfileLevel.AACObjectLC)
      setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, minBufSize)
    }

    val encoder = MediaCodec.createEncoderByType(MediaFormat.MIMETYPE_AUDIO_AAC)
    encoder.configure(audioMediaFormat, null, null, MediaCodec.CONFIGURE_FLAG_ENCODE)
    encoder.start()
    audioEncoder = encoder

    val recorder = AudioRecord(
      android.media.MediaRecorder.AudioSource.MIC,
      sampleRate, channelConfig, audioFormat, minBufSize * 2,
    )
    audioRecord = recorder
    recorder.startRecording()

    var audioConfigSent = false

    audioThread = Thread({
      val bufferInfo = MediaCodec.BufferInfo()
      val readBuffer = ByteArray(minBufSize)

      while (!stopRequested) {
        val readSize = recorder.read(readBuffer, 0, minBufSize)
        if (readSize <= 0) continue

        val inputBufferIndex = encoder.dequeueInputBuffer(10000)
        if (inputBufferIndex >= 0) {
          val inputBuffer = encoder.getInputBuffer(inputBufferIndex)
          inputBuffer?.clear()
          inputBuffer?.put(readBuffer, 0, readSize)
          encoder.queueInputBuffer(inputBufferIndex, 0, readSize, System.nanoTime() / 1000, 0)
        }

        while (true) {
          val outputBufferIndex = encoder.dequeueOutputBuffer(bufferInfo, 0)
          when {
            outputBufferIndex >= 0 -> {
              val outputBuffer = encoder.getOutputBuffer(outputBufferIndex)
              if (outputBuffer != null && bufferInfo.size > 0) {
                val data = ByteArray(bufferInfo.size)
                outputBuffer.position(bufferInfo.offset)
                outputBuffer.get(data)
                val isConfig = (bufferInfo.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG) != 0
                if (isConfig) audioConfigSent = true
                client.publishAudioData(data, bufferInfo.presentationTimeUs, isConfig)
              }
              encoder.releaseOutputBuffer(outputBufferIndex, false)
            }
            outputBufferIndex == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED -> {
              Log.w("RtmpStreamer", "Audio format changed")
              val outFormat = encoder.outputFormat
              val csd0 = outFormat.getByteBuffer("csd-0")
              if (csd0 != null && !audioConfigSent) {
                val csdData = ByteArray(csd0.remaining())
                csd0.get(csdData)
                client.publishAudioData(csdData, 0, isConfig = true)
                audioConfigSent = true
              }
            }
            outputBufferIndex == MediaCodec.INFO_TRY_AGAIN_LATER -> break
          }
        }
      }

      try { recorder.stop() } catch (_: Exception) {}
      try { recorder.release() } catch (_: Exception) {}
      try { encoder.stop() } catch (_: Exception) {}
      try { encoder.release() } catch (_: Exception) {}
    }, "rtmp-audio").also { it.start() }
  }

  fun stop() {
    synchronized(this) {
      stopRequested = true
      isStreaming = false
    }
    videoThread?.join(5000)
    audioThread?.join(5000)

    // Unbind camera on main thread
    val camProvider = cameraProvider
    if (camProvider != null) {
      val latch = CountDownLatch(1)
      mainHandler.post {
        try { camProvider.unbindAll() } catch (_: Exception) {}
        latch.countDown()
      }
      latch.await(5, TimeUnit.SECONDS)
    }

    try { rtmpClient?.close() } catch (_: Exception) {}
    videoEncoder = null; audioEncoder = null; audioRecord = null
    cameraProvider = null; rtmpClient = null; videoThread = null; audioThread = null
  }

  fun isActive(): Boolean = isStreaming && !stopRequested

  /** Minimal LifecycleOwner for CameraX binding without Activity. */
  private class SimpleLifecycleOwner : LifecycleOwner {
    private val _lifecycle = LifecycleRegistry(this)
    override val lifecycle: Lifecycle
      get() = _lifecycle.also { it.currentState = Lifecycle.State.RESUMED }
  }
}
