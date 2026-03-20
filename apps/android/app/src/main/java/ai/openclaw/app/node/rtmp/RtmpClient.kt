package ai.openclaw.app.node.rtmp

import android.util.Log
import java.io.InputStream
import java.io.OutputStream
import java.net.Socket
import java.net.URI
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Pure Java RTMP client — no third-party dependencies.
 *
 * Implements:
 *  1. RTMP Handshake (C0/C1/C2)
 *  2. AMF0 Connect + createStream + publish
 *  3. FLV tag muxing (H264 + AAC) over RTMP chunks
 */
@Suppress("TooManyFunctions", "MagicNumber")
class RtmpClient(private val listener: Listener) {

  interface Listener {
    fun onConnected()
    fun onConnectionFailed(reason: String)
    fun onDisconnected()
  }

  private var socket: Socket? = null
  private var out: OutputStream? = null
  private var input: InputStream? = null
  private val connected = AtomicBoolean(false)
  private val streaming = AtomicBoolean(false)

  // Chunk encoding state
  private var chunkSizeOut = 4096
  private var windowAckSize = 2500000L
  private var bytesOut = 0L
  private var txnId = 1.0
  private var streamId = 1.0

  companion object {
    private const val TAG = "RtmpClient"

    // RTMP message types
    private const val MSG_SET_CHUNK_SIZE    = 1
    private const val MSG_ACK              = 3
    private const val MSG_USER_CONTROL     = 4
    private const val MSG_WINDOW_ACK_SIZE  = 5
    private const val MSG_AUDIO            = 8
    private const val MSG_VIDEO            = 9
    private const val MSG_COMMAND          = 20

    // Chunk stream IDs
    private const val CSID_PROTOCOL  = 2
    private const val CSID_INVOKE    = 3
    private const val CSID_AUDIO     = 4
    private const val CSID_VIDEO     = 6

    // AMF0 markers
    private const val AMF_NUMBER  = 0x00.toByte()
    private const val AMF_BOOLEAN = 0x01.toByte()
    private const val AMF_STRING  = 0x02.toByte()
    private const val AMF_OBJECT  = 0x03.toByte()
    private const val AMF_NULL    = 0x05.toByte()
    private const val AMF_ECMA_ARRAY = 0x08.toByte()
    private const val AMF_END     = 0x09.toByte()
  }

  // ===================== Public API =====================

  @Synchronized
  fun connect(rtmpUrl: String) {
    if (connected.get()) return
    try {
      val (host, port, app, stream) = parseUrl(rtmpUrl)
      Log.i(TAG, "Connecting to $host:$port app=$app stream=$stream")

      socket = Socket().apply {
        connect(java.net.InetSocketAddress(host, port), 10000)
        soTimeout = 0
      }
      out = socket!!.getOutputStream()
      input = socket!!.getInputStream()

      handshake()
      doConnect(host, app)
      doCreateStream()
      doPublish(stream)

      connected.set(true)
      streaming.set(true)
      Log.i(TAG, "RTMP streaming to $app/$stream")
      listener.onConnected()
    } catch (e: Exception) {
      Log.e(TAG, "Connect failed", e)
      close()
      listener.onConnectionFailed(e.message ?: "unknown")
    }
  }

  fun isConnected() = connected.get()
  fun isStreaming() = streaming.get()

  /**
   * Send H264 video data as FLV tag.
   * @param data raw NAL units (format 4-byte length prefix)
   * @param timestampUs presentation timestamp in microseconds
   * @param flags MediaCodec.BufferInfo.flags
   */
  fun publishVideoData(data: ByteArray, timestampUs: Long, flags: Int) {
    if (!streaming.get()) return
    try {
      val ts = (timestampUs / 1000).toInt() and 0x7FFFFFFF
      val isKeyFrame = (flags and 0x01) != 0 // BUFFER_FLAG_KEY_FRAME
      val isConfig = (flags and 0x02) != 0   // BUFFER_FLAG_CODEC_CONFIG

      if (isConfig) {
        // Parse SPS/PPS from AVCC format (4-byte length prefix) and send sequence header
        val flv = buildAvcSequenceHeader(data)
        sendRtmpMessage(CSID_VIDEO, MSG_VIDEO, flv, ts)
      } else {
        // Build FLV VideoTagBody: FrameType|CodecID + AVC NALU header + composition time + NALUs
        val frameType = if (isKeyFrame) 0x17 else 0x27 // keyframe=1, codec=7 (AVC)
        val naluHeader = byteArrayOf(
          frameType.toByte(),  // FrameType(4bits) + CodecID(4bits)
          0x01,               // AVC packet type = NALU
          0x00, 0x00, 0x00    // composition time offset = 0
        )
        val flv = naluHeader + data
        sendRtmpMessage(CSID_VIDEO, MSG_VIDEO, flv, ts)
      }
    } catch (e: Exception) {
      Log.e(TAG, "publishVideoData error", e)
    }
  }

  /**
   * Send AAC audio data as FLV tag.
   * @param data encoded AAC frames
   * @param timestampUs presentation timestamp in microseconds
   * @param isConfig true for AudioSpecificConfig
   */
  fun publishAudioData(data: ByteArray, timestampUs: Long, isConfig: Boolean = false) {
    if (!streaming.get()) return
    try {
      val ts = (timestampUs / 1000).toInt() and 0x7FFFFFFF
      val soundType = 0x0F // SoundType=1(stereo), SoundSize=1(16-bit), SoundRate=3(44kHz), SoundFormat=10(AAC)
      val flv = byteArrayOf(
        soundType.toByte(),
        if (isConfig) 0x00 else 0x01 // AAC packet type
      ) + data
      sendRtmpMessage(CSID_AUDIO, MSG_AUDIO, flv, ts)
    } catch (e: Exception) {
      Log.e(TAG, "publishAudioData error", e)
    }
  }

  @Synchronized
  fun close() {
    streaming.set(false)
    connected.set(false)
    try { socket?.close() } catch (_: Exception) {}
    socket = null; out = null; input = null
    Log.i(TAG, "Closed")
    listener.onDisconnected()
  }

  fun stop() = close()

  // ===================== RTMP Handshake =====================

  private fun handshake() {
    val o = out!!
    val i = input!!

    // C0
    o.write(0x03); o.flush()

    // C1: timestamp(4) + zero(4) + random(1528)
    val c1 = ByteArray(1536)
    ByteBuffer.wrap(c1).order(ByteOrder.BIG_ENDIAN)
      .putInt((System.currentTimeMillis() / 1000).toInt())
    // Fill random bytes from offset 8 to end
    val random = SecureRandom()
    for (i in 8 until 1536) c1[i] = (java.util.Random().nextInt(256)).toByte()
    o.write(c1); o.flush()

    // S0
    val s0 = ByteArray(1); readFully(i, s0)
    check(s0[0].toInt() == 3) { "Unsupported RTMP version: ${s0[0]}" }

    // S1
    val s1 = ByteArray(1536); readFully(i, s1)

    // C2 = echo S1
    o.write(s1); o.flush()

    // S2
    val s2 = ByteArray(1536); readFully(i, s2)
    Log.i(TAG, "Handshake done")
  }

  // ===================== AMF0 Connect =====================

  private fun doConnect(host: String, app: String) {
    sendCommand(CSID_INVOKE, "connect", txnId++) {
      amfEcmaArray {
        put("app", app)
        put("flashVer", "FMLE/3.0 (compatible; FMSc/1.0)")
        put("tcUrl", "rtmp://$host/$app")
        put("fpad", false)
        put("capabilities", 239)
        put("audioCodecs", 3575)
        put("videoCodecs", 252)
        put("videoFunction", 1)
        put("pageUrl", "")
        put("swfUrl", "")
      }
    }
    drainServerMessages(5000)
  }

  private fun doCreateStream() {
    sendCommand(CSID_INVOKE, "createStream", txnId++) {
      amfNull()
    }
    // Try to read stream ID from response
    try {
      val resp = readOneMessage(5000)
      if (resp != null) {
        // Parse: _result(string), txnId(number), null, streamId(number)
        val amf = AmfReader(resp.data)
        try {
          amf.skip() // "_result"
          amf.skip() // txnId
          amf.skip() // null
          streamId = amf.readNumber()
          Log.i(TAG, "Stream id=$streamId")
        } catch (_: Exception) {}
      }
    } catch (_: Exception) {}
  }

  private fun doPublish(streamName: String) {
    sendCommand(CSID_INVOKE, "publish", 0.0) {
      amfString(streamName)
      amfString("live")
    }
    Log.i(TAG, "Publishing stream '$streamName'")
  }

  // ===================== RTMP Chunk Protocol =====================

  @Synchronized
  private fun sendRtmpMessage(csid: Int, msgType: Int, data: ByteArray, timestamp: Int) {
    val o = out ?: return
    try {
      writeChunkHeader(o, csid, msgType, data.size, timestamp, fmt = 0)
      writeChunked(o, data, csid)
      o.flush()

      bytesOut += data.size
      if (bytesOut >= windowAckSize) {
        val ack = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(bytesOut.toInt()).array()
        writeChunkHeader(o, CSID_PROTOCOL, MSG_ACK, ack.size, 0, fmt = 0)
        o.write(ack); o.flush()
        bytesOut = 0
      }
    } catch (e: Exception) {
      Log.e(TAG, "sendRtmpMessage error", e)
    }
  }

  private fun writeChunkHeader(o: OutputStream, csid: Int, msgType: Int, len: Int, timestamp: Int, fmt: Int) {
    // Basic header
    when {
      csid in 2..63 -> o.write((fmt shl 6) or csid)
      csid <= 319 -> { o.write((fmt shl 6) or 0); o.write(csid - 64) }
      else -> { o.write((fmt shl 6) or 1); val v = csid - 32; o.write(v shr 8); o.write(v and 0xFF) }
    }
    if (fmt == 0) {
      // Message header: timestamp(3) + length(3) + type(1) + streamId(4 LE)
      val h = ByteBuffer.allocate(11).order(ByteOrder.BIG_ENDIAN)
      h.putInt(timestamp); h.put(h.get(1).toInt().toByte()); h.put(h.get(2).toInt().toByte()); h.put(h.get(3).toInt().toByte())
      // Redo properly
    }
    // Proper fmt=0 header: timestamp(3 bytes) + message_length(3 bytes) + message_type_id(1 byte) + stream_id(4 bytes LE)
    val h = ByteArray(11)
    h[0] = ((timestamp shr 16) and 0xFF).toByte()
    h[1] = ((timestamp shr 8) and 0xFF).toByte()
    h[2] = (timestamp and 0xFF).toByte()
    h[3] = ((len shr 16) and 0xFF).toByte()
    h[4] = ((len shr 8) and 0xFF).toByte()
    h[5] = (len and 0xFF).toByte()
    h[6] = msgType.toByte()
    // stream_id little-endian
    val sid = streamId.toBits()
    h[7] = (sid and 0xFF).toByte()
    h[8] = ((sid shr 8) and 0xFF).toByte()
    h[9] = ((sid shr 16) and 0xFF).toByte()
    h[10] = ((sid shr 24) and 0xFF).toByte()
    o.write(h)
  }

  private fun writeChunked(o: OutputStream, data: ByteArray, csid: Int) {
    var offset = 0
    while (offset < data.size) {
      val len = minOf(data.size - offset, chunkSizeOut)
      o.write(data, offset, len)
      offset += len
      if (offset < data.size) {
        // fmt=3 continuation header (just basic header)
        when {
          csid in 2..63 -> o.write(csid)
          csid <= 319 -> { o.write(0); o.write(csid - 64) }
          else -> { o.write(1); val v = csid - 32; o.write(v shr 8); o.write(v and 0xFF) }
        }
      }
    }
  }

  // ===================== AMF0 Helpers =====================

  private fun sendCommand(csid: Int, command: String, txn: Double, body: AmfWriter.() -> Unit) {
    val w = AmfWriter()
    w.amfString(command)
    w.amfNumber(txn)
    w.body()
    sendRtmpMessage(csid, MSG_COMMAND, w.toByteArray(), 0)
  }

  private class AmfWriter {
    private val chunks = mutableListOf<ByteArray>()
    private var current = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN)

    fun amfString(s: String) {
      ensure(1 + 2 + s.toByteArray(Charsets.UTF_8).size)
      current.put(AMF_STRING)
      val b = s.toByteArray(Charsets.UTF_8)
      current.putShort(b.size.toShort())
      current.put(b)
    }

    fun amfNumber(n: Double) {
      ensure(9)
      current.put(AMF_NUMBER)
      current.putDouble(n)
    }

    fun amfBoolean(v: Boolean) {
      ensure(2)
      current.put(AMF_BOOLEAN)
      current.put(if (v) 0x01 else 0x00)
    }

    fun amfNull() {
      ensure(1)
      current.put(AMF_NULL)
    }

    fun amfEcmaArray(body: AmfWriter.() -> Unit) {
      ensure(5)
      current.put(AMF_ECMA_ARRAY)
      current.putInt(0) // approximate count
      this.body()
      current.putShort(0) // end marker
      current.put(AMF_END)
    }

    fun put(key: String, value: String) {
      val b = key.toByteArray(Charsets.UTF_8)
      ensure(2 + b.size)
      current.putShort(b.size.toShort())
      current.put(b)
      amfString(value)
    }

    fun put(key: String, value: Boolean) {
      val b = key.toByteArray(Charsets.UTF_8)
      ensure(2 + b.size)
      current.putShort(b.size.toShort())
      current.put(b)
      amfBoolean(value)
    }

    fun put(key: String, value: Int) {
      val b = key.toByteArray(Charsets.UTF_8)
      ensure(2 + b.size)
      current.putShort(b.size.toShort())
      current.put(b)
      amfNumber(value.toDouble())
    }

    private fun ensure(n: Int) {
      if (current.remaining() < n) {
        flush()
      }
      if (current.remaining() < n) {
        current = ByteBuffer.allocate(maxOf(current.capacity() * 2, n + 256)).order(ByteOrder.BIG_ENDIAN)
      }
    }

    private fun flush() {
      if (current.position() > 0) {
        val arr = ByteArray(current.position())
        current.flip(); current.get(arr)
        chunks.add(arr)
        current.clear()
      }
    }

    fun toByteArray(): ByteArray {
      flush()
      return chunks.reduce { a, b -> a + b }
    }
  }

  // ===================== Server Message Reader =====================

  /**
   * Read and process incoming server messages for a limited time.
   * Handles SetChunkSize and WindowAckSize automatically.
   */
  private fun drainServerMessages(timeoutMs: Long) {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline) {
      try {
        val msg = readOneMessage(500)
        if (msg == null) break
        if (msg.msgType == MSG_SET_CHUNK_SIZE && msg.data.size >= 4) {
          chunkSizeOut = ByteBuffer.wrap(msg.data).order(ByteOrder.BIG_ENDIAN).int and 0x7FFFFFFF
          Log.i(TAG, "Server chunk size: $chunkSizeOut")
        } else if (msg.msgType == MSG_WINDOW_ACK_SIZE && msg.data.size >= 4) {
          windowAckSize = ByteBuffer.wrap(msg.data).order(ByteOrder.BIG_ENDIAN).int.toLong() and 0xFFFFFFFFL
          Log.i(TAG, "Window ack size: $windowAckSize")
        }
      } catch (_: Exception) { break }
    }
  }

  private data class ServerMessage(val msgType: Int, val data: ByteArray)

  private fun readOneMessage(timeoutMs: Long): ServerMessage? {
    val i = input ?: return null
    val deadline = System.currentTimeMillis() + timeoutMs
    try {
      while (i.available() <= 0 && System.currentTimeMillis() < deadline) Thread.sleep(10)
      if (i.available() <= 0) return null

      // Basic header
      val fb = i.read()
      if (fb < 0) return null
      val fmt = (fb shr 6) and 0x03
      var csid = fb and 0x3F
      if (csid == 0) { val b = i.read(); if (b < 0) return null; csid = (b and 0xFF) + 64 }
      else if (csid == 1) { val b1 = i.read(); val b2 = i.read(); if (b1 < 0 || b2 < 0) return null; csid = ((b1 and 0xFF) shl 8) or (b2 and 0xFF) + 32 }

      // Message header (fmt 0 = 11 bytes, fmt 1 = 7, fmt 2 = 3, fmt 3 = 0)
      var timestamp = 0; var msgLen = 0; var msgType = 0
      if (fmt <= 2) {
        timestamp = (readByte(i, deadline) shl 16) or (readByte(i, deadline) shl 8) or readByte(i, deadline)
      }
      if (fmt <= 1) {
        msgLen = (readByte(i, deadline) shl 16) or (readByte(i, deadline) shl 8) or readByte(i, deadline)
        msgType = readByte(i, deadline)
      }
      if (fmt == 0) {
        // stream id (4 bytes LE) — skip
        readByte(i, deadline); readByte(i, deadline); readByte(i, deadline); readByte(i, deadline)
      }
      if (timestamp == 0xFFFFFF) {
        timestamp = (readByte(i, deadline) shl 24) or (readByte(i, deadline) shl 16) or (readByte(i, deadline) shl 8) or readByte(i, deadline)
      }

      if (msgLen <= 0 || msgLen > 1_000_000) return null

      // Read body (handle chunking)
      val body = ByteArray(msgLen)
      var off = 0
      while (off < msgLen && System.currentTimeMillis() < deadline) {
        val toRead = minOf(msgLen - off, chunkSizeOut)
        val n = i.read(body, off, toRead)
        if (n <= 0) break
        off += n
        if (off < msgLen) {
          // Skip continuation basic header
          readByte(i, deadline)
        }
      }

      return if (off > 0) ServerMessage(msgType, body.copyOf(off)) else null
    } catch (_: Exception) { return null }
  }

  private fun readByte(i: InputStream, deadline: Long): Int {
    while (i.available() <= 0 && System.currentTimeMillis() < deadline) Thread.sleep(5)
    return i.read()
  }

  private fun readFully(i: InputStream, buf: ByteArray) {
    var off = 0
    val deadline = System.currentTimeMillis() + 10000
    while (off < buf.size) {
      if (System.currentTimeMillis() > deadline) throw IllegalStateException("Handshake timeout")
      val n = i.read(buf, off, buf.size - off)
      if (n <= 0) throw IllegalStateException("Connection closed")
      off += n
    }
  }

  // ===================== URL Parsing =====================

  private fun parseUrl(url: String): Tuple4<String, Int, String, String> {
    val clean = url.removePrefix("rtmp://").removePrefix("rtmps://")
    val uri = URI("http://$clean")
    val host = uri.host
    val port = if (uri.port > 0) uri.port else 1935
    val path = uri.path.removePrefix("/")
    val parts = path.split("/", limit = 2)
    val app = parts.getOrNull(0) ?: "live"
    val stream = parts.getOrNull(1) ?: "stream"
    // Strip query string from stream
    val streamClean = stream.split("?").first()
    return Tuple4(host, port, app, streamClean)
  }

  private data class Tuple4<A, B, C, D>(val a: A, val b: B, val c: C, val d: D)

  // ===================== FLV Helpers =====================

  /**
   * Build FLV AVC Sequence Header from raw AVCC data (4-byte length prefixed SPS/PPS).
   */
  private fun buildAvcSequenceHeader(data: ByteArray): ByteArray {
    // data format: length(4) + SPS, length(4) + PPS
    if (data.size < 8) return data
    val buf = ByteBuffer.allocate(data.size + 16).order(ByteOrder.BIG_ENDIAN)

    buf.put(0x17) // keyframe(1) + AVC(7)
    buf.put(0x00) // AVC sequence header
    buf.put(0x00); buf.put(0x00); buf.put(0x00) // composition time = 0

    // AVCDecoderConfigurationRecord
    buf.put(0x01) // configurationVersion
    buf.put(data[4]) // AVCProfileIndication (from SPS byte 1)
    buf.put(data[5]) // profile_compatibility
    buf.put(data[6]) // AVCLevelIndication
    buf.put(0xFF.toByte()) // lengthSizeMinusOne = 3 (4 bytes NAL length)

    var pos = 0
    // Count SPS
    var numSps = 0
    var spsEnd = 0
    while (pos < data.size - 4) {
      val nalLen = ByteBuffer.wrap(data, pos, 4).order(ByteOrder.BIG_ENDIAN).int
      if (nalLen <= 0 || pos + 4 + nalLen > data.size) break
      val nalType = data[pos + 4].toInt() and 0x1F
      if (nalType == 7) numSps++
      pos += 4 + nalLen
      if (nalType == 7) spsEnd = pos
    }

    buf.put(numSps.toByte())
    // SPS
    pos = 0
    while (pos < spsEnd) {
      val nalLen = ByteBuffer.wrap(data, pos, 4).order(ByteOrder.BIG_ENDIAN).int
      if (nalLen <= 0 || pos + 4 + nalLen > data.size) break
      val nalType = data[pos + 4].toInt() and 0x1F
      if (nalType == 7) {
        buf.putShort(nalLen.toShort())
        buf.put(data, pos + 4, nalLen)
      }
      pos += 4 + nalLen
    }

    // PPS
    buf.put(0x01) // numPPS = 1
    while (pos < data.size - 4) {
      val nalLen = ByteBuffer.wrap(data, pos, 4).order(ByteOrder.BIG_ENDIAN).int
      if (nalLen <= 0 || pos + 4 + nalLen > data.size) break
      val nalType = data[pos + 4].toInt() and 0x1F
      if (nalType == 8) {
        buf.putShort(nalLen.toShort())
        buf.put(data, pos + 4, nalLen)
        break
      }
      pos += 4 + nalLen
    }

    val result = ByteArray(buf.position())
    buf.flip(); buf.get(result)
    return result
  }

  // ===================== AMF0 Reader =====================

  private class AmfReader(val data: ByteArray) {
    var pos = 0
    fun skip() {
      if (pos >= data.size) return
      val t = data[pos++]
      when (t.toInt()) {
        AMF_NUMBER.toInt() -> pos += 8
        AMF_STRING.toInt() -> {
          val len = ((data[pos].toInt() and 0xFF) shl 8) or (data[pos + 1].toInt() and 0xFF)
          pos += 2 + len
        }
        AMF_BOOLEAN.toInt() -> pos += 1
        AMF_NULL.toInt() -> {}
        AMF_OBJECT.toInt(), AMF_ECMA_ARRAY.toInt() -> {
          if (t.toInt() == AMF_ECMA_ARRAY.toInt()) pos += 4 // count
          while (pos < data.size - 2) {
            val kLen = ((data[pos].toInt() and 0xFF) shl 8) or (data[pos + 1].toInt() and 0xFF)
            pos += 2
            if (kLen == 0 && data[pos].toInt() == AMF_END.toInt()) { pos++; break }
            pos += kLen
            skip()
          }
        }
      }
    }
    fun readNumber(): Double {
      pos++ // skip marker
      return ByteBuffer.wrap(data, pos, 8).order(ByteOrder.BIG_ENDIAN).double.also { pos += 8 }
    }
  }

  private fun Double.toBits(): Long = java.lang.Double.doubleToRawLongBits(this)
}
