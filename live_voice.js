// Live voice conversation with Gemini (Live API), used by the AI Speaking
// Partner's voice mode. Talks to Google directly with the student's own key;
// nothing goes through our servers. Same audio pipeline as the tested
// live-test.html page: mic -> 16 kHz PCM -> WebSocket -> 24 kHz PCM -> speaker.
(function () {
  const WS_BASE = 'wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent';
  let ws = null, ctx = null, stream = null, processor = null, source = null, sink = null;
  let playHead = 0, playing = [], onEvent = function () {};

  function emit(type, text) { try { onEvent(type, text == null ? '' : String(text)); } catch (e) {} }

  function toBase64(bytes) {
    let bin = '';
    for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
    return btoa(bin);
  }

  function to16kPcm(input, inRate) {
    const ratio = inRate / 16000;
    const outLen = Math.floor(input.length / ratio);
    const out = new Int16Array(outLen);
    for (let i = 0; i < outLen; i++) {
      const start = Math.floor(i * ratio), end = Math.min(input.length, Math.floor((i + 1) * ratio));
      let sum = 0;
      for (let j = start; j < end; j++) sum += input[j];
      const v = Math.max(-1, Math.min(1, sum / Math.max(1, end - start)));
      out[i] = v < 0 ? v * 0x8000 : v * 0x7fff;
    }
    return new Uint8Array(out.buffer);
  }

  function aiSpeaking() { return ctx && playHead > ctx.currentTime + 0.05; }

  function playPcm(b64) {
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    const samples = new Int16Array(bytes.buffer, 0, Math.floor(bytes.length / 2));
    if (samples.length === 0) return;
    const buf = ctx.createBuffer(1, samples.length, 24000);
    const ch = buf.getChannelData(0);
    for (let i = 0; i < samples.length; i++) ch[i] = samples[i] / 0x8000;
    const node = ctx.createBufferSource();
    node.buffer = buf;
    node.connect(ctx.destination);
    playHead = Math.max(playHead, ctx.currentTime + 0.05);
    node.start(playHead);
    playHead += buf.duration;
    playing.push(node);
    emit('status', 'speaking');
    node.onended = function () {
      playing = playing.filter(function (n) { return n !== node; });
      if (playing.length === 0 && ws && ws.readyState === WebSocket.OPEN) emit('status', 'listening');
    };
  }

  function stopPlayback() {
    playing.forEach(function (n) { try { n.stop(); } catch (e) {} });
    playing = [];
    if (ctx) playHead = ctx.currentTime;
  }

  async function handleMessage(event) {
    const text = typeof event.data === 'string' ? event.data : await event.data.text();
    let msg;
    try { msg = JSON.parse(text); } catch (e) { return; }
    if (msg.setupComplete) { emit('status', 'listening'); return; }
    const sc = msg.serverContent;
    if (sc) {
      if (sc.interrupted) stopPlayback();
      if (sc.inputTranscription && sc.inputTranscription.text) emit('user', sc.inputTranscription.text);
      if (sc.outputTranscription && sc.outputTranscription.text) emit('ai', sc.outputTranscription.text);
      const parts = (sc.modelTurn && sc.modelTurn.parts) || [];
      for (const p of parts) {
        if (p.inlineData && p.inlineData.data && (p.inlineData.mimeType || '').startsWith('audio/')) playPcm(p.inlineData.data);
      }
      if (sc.turnComplete) emit('turn', '');
    }
    if (msg.goAway) emit('status', 'ending');
  }

  function cleanupAudio() {
    try { processor && processor.disconnect(); } catch (e) {}
    try { source && source.disconnect(); } catch (e) {}
    try { sink && sink.disconnect(); } catch (e) {}
    if (stream) stream.getTracks().forEach(function (t) { t.stop(); });
    processor = source = sink = stream = null;
  }

  function stop() {
    stopPlayback();
    const socket = ws;
    ws = null;
    if (socket) { try { socket.close(1000); } catch (e) {} }
    cleanupAudio();
    if (ctx) { try { ctx.close(); } catch (e) {} ctx = null; }
    playHead = 0;
  }

  // Must be called from a tap (iPhone only allows audio/mic after a gesture).
  async function start(key, model, systemPrompt, callback) {
    stop();
    onEvent = typeof callback === 'function' ? callback : function () {};
    try {
      ctx = new (window.AudioContext || window.webkitAudioContext)();
      playHead = 0; // new audio clock: forget the last session's timing
      playing = [];
      await ctx.resume();
      stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, channelCount: 1 } });
    } catch (e) {
      stop();
      emit('error', 'mic:' + (e && e.name ? e.name : 'Error'));
      return;
    }
    emit('status', 'connecting');
    const socket = new WebSocket(WS_BASE + '?key=' + encodeURIComponent(key));
    ws = socket;
    socket.onopen = function () {
      socket.send(JSON.stringify({ setup: {
        model: 'models/' + model,
        generationConfig: { responseModalities: ['AUDIO'] },
        systemInstruction: { parts: [{ text: systemPrompt }] },
        inputAudioTranscription: {},
        outputAudioTranscription: {},
      } }));
      source = ctx.createMediaStreamSource(stream);
      processor = ctx.createScriptProcessor(4096, 1, 1);
      sink = ctx.createGain(); sink.gain.value = 0;
      processor.onaudioprocess = function (e) {
        if (ws !== socket || socket.readyState !== WebSocket.OPEN) return;
        if (aiSpeaking()) return; // pause the mic while the AI talks (no echo)
        const pcm = to16kPcm(e.inputBuffer.getChannelData(0), ctx.sampleRate);
        socket.send(JSON.stringify({ realtimeInput: { audio: { data: toBase64(pcm), mimeType: 'audio/pcm;rate=16000' } } }));
      };
      source.connect(processor); processor.connect(sink); sink.connect(ctx.destination);
    };
    socket.onmessage = handleMessage;
    socket.onclose = function (e) {
      if (ws === socket) { ws = null; cleanupAudio(); if (ctx) { try { ctx.close(); } catch (x) {} ctx = null; } playHead = 0; }
      emit('closed', e.code + (e.reason ? ' ' + e.reason : ''));
    };
  }

  function supported() {
    return !!(window.WebSocket && navigator.mediaDevices && navigator.mediaDevices.getUserMedia &&
      (window.AudioContext || window.webkitAudioContext));
  }

  window.liveVoice = { start: start, stop: stop, supported: supported };
})();
