#!/usr/bin/env python3
"""
GhostChat - Chat ứng dụng realtime với WebSocket
Chạy trên Linux, Replit, Termux, Windows
"""

import asyncio
import json
import logging
import os
import random
import re
import shutil
import signal
import socket
import string
import sys
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Set
import mimetypes

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
import uvicorn

# ===================================================
# Tắt log
# ===================================================
logging.getLogger("uvicorn").setLevel(logging.CRITICAL)
logging.getLogger("uvicorn.access").setLevel(logging.CRITICAL)
logging.getLogger("uvicorn.error").setLevel(logging.CRITICAL)
logging.getLogger("fastapi").setLevel(logging.CRITICAL)
logging.getLogger("asyncio").setLevel(logging.CRITICAL)

# ===================================================
# Cấu hình
# ===================================================
DEFAULT_PORT = 8000
MAX_FILE_SIZE = 100 * 1024 * 1024
ROOM_CODE_LENGTH = 16
TEMP_DIR = Path("temp")

# ===================================================
# Global State
# ===================================================
def generate_room_code() -> str:
    chars = string.ascii_letters + string.digits + "-_"
    return ''.join(random.choice(chars) for _ in range(ROOM_CODE_LENGTH))

ROOM_KEY = generate_room_code()
MESSAGES: List[dict] = []
CLIENTS: Set[WebSocket] = set()
FILES_DIR: Path = TEMP_DIR / ROOM_KEY / "files"
SHUTDOWN_DONE = False

try:
    FILES_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass

app = FastAPI(title="GhostChat", version="1.0.0")

# ===================================================
# HTML Template
# ===================================================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>GhostChat</title>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:Arial,sans-serif;background:#0a0a0a;color:#e4e6eb;height:100vh;overflow:hidden}
        .screen{display:none;width:100%;height:100vh;position:fixed;top:0;left:0;background:#0a0a0a}
        .screen.active{display:flex;justify-content:center;align-items:center}
        .login-container{max-width:420px;width:90%;padding:40px 30px;background:#1a1a1a;border-radius:20px;text-align:center}
        .logo{font-size:72px;display:block;margin-bottom:10px}
        .app-title{font-size:36px;font-weight:700;background:linear-gradient(135deg,#667eea,#764ba2);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:30px}
        .room-input{width:100%;padding:16px;background:#2a2a2a;border:2px solid #3a3a3a;border-radius:12px;color:#e4e6eb;font-size:18px;text-align:center;text-transform:uppercase;outline:none}
        .room-input:focus{border-color:#667eea}
        .join-btn{width:100%;padding:16px;margin-top:12px;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border:none;border-radius:12px;font-size:18px;font-weight:600;cursor:pointer}
        .join-btn:disabled{opacity:.6;cursor:not-allowed}
        .error-message{color:#ff6b6b;font-size:14px;margin-top:10px;display:none}
        .error-message.show{display:block}
        .chat-container{width:100%;height:100vh;max-width:800px;margin:0 auto;display:flex;flex-direction:column;background:#0f0f0f}
        .chat-header{display:flex;justify-content:space-between;align-items:center;padding:16px 20px;background:#1a1a1a;border-bottom:1px solid #2a2a2a}
        .room-code{color:#667eea;font-weight:600;background:#2a2a2a;padding:4px 12px;border-radius:6px}
        .logout-btn{width:36px;height:36px;background:#2a2a2a;border:none;border-radius:50%;color:#8a8a8a;font-size:20px;cursor:pointer}
        .logout-btn:hover{background:#3a2a2a;color:#ff6b6b}
        .messages-container{flex:1;overflow-y:auto;padding:20px;background:#0f0f0f}
        .messages-list{display:flex;flex-direction:column;gap:8px}
        .message{max-width:75%;padding:10px 14px;border-radius:18px;animation:slideIn .3s ease}
        @keyframes slideIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
        .message.self{align-self:flex-end;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border-bottom-right-radius:4px}
        .message.other{align-self:flex-start;background:#1e1e1e;border-bottom-left-radius:4px}
        .message.system{align-self:center;background:#1a1a1a;color:#8a8a8a;font-size:13px;padding:6px 16px;border-radius:20px}
        .message .username{font-size:13px;font-weight:600;color:#667eea}
        .message.other .username{color:#8a8a8a}
        .message .content{font-size:15px}
        .message .timestamp{font-size:11px;opacity:.6;margin-top:4px;text-align:right;display:block}
        .message .file-container{background:rgba(0,0,0,.2);border-radius:12px;padding:12px;cursor:pointer}
        .message .file-preview{max-width:100%;border-radius:8px}
        .message .file-preview.image{max-height:300px}
        .message .file-preview.video{max-height:300px;width:100%}
        .message .file-info{display:flex;align-items:center;gap:8px;font-size:13px;margin-top:8px}
        .typing-indicator{display:none;padding:8px 16px;color:#8a8a8a;font-style:italic}
        .typing-indicator.show{display:block}
        .input-area{padding:12px 20px 20px;background:#1a1a1a;border-top:1px solid #2a2a2a}
        .input-wrapper{display:flex;align-items:center;gap:10px;background:#2a2a2a;border-radius:25px;padding:6px 8px 6px 16px;border:2px solid #3a3a3a}
        .input-wrapper:focus-within{border-color:#667eea}
        .file-btn{width:40px;height:40px;border-radius:50%;border:none;background:transparent;color:#8a8a8a;font-size:24px;cursor:pointer}
        .file-btn:hover{color:#667eea}
        .message-input{flex:1;background:transparent;border:none;outline:none;color:#e4e6eb;font-size:16px;padding:8px 0}
        .send-btn{width:40px;height:40px;border-radius:50%;border:none;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;font-size:20px;cursor:pointer}
        .send-btn:disabled{opacity:.5;cursor:not-allowed}
        .upload-progress{display:none;margin-top:8px;padding:8px 12px;background:#2a2a2a;border-radius:8px;align-items:center;gap:12px}
        .upload-progress.show{display:flex}
        .progress-bar{flex:1;height:6px;background:#3a3a3a;border-radius:3px;overflow:hidden}
        .progress-fill{height:100%;background:linear-gradient(135deg,#667eea,#764ba2);width:0;transition:width .3s}
        .progress-text{color:#8a8a8a;font-size:13px;min-width:40px}
        .file-input{display:none}
        .loading-spinner{display:inline-block;width:20px;height:20px;border:3px solid rgba(102,126,234,.2);border-top-color:#667eea;border-radius:50%;animation:spin .6s linear infinite}
        @keyframes spin{to{transform:rotate(360deg)}}
        @media(max-width:768px){.login-container{padding:30px 20px}.logo{font-size:60px}.app-title{font-size:28px}.message{max-width:85%}}
        @media(max-width:480px){.login-container{padding:24px 16px}.logo{font-size:48px}.app-title{font-size:24px}.message{max-width:90%}}
    </style>
</head>
<body>
<div id="loginScreen" class="screen active">
    <div class="login-container">
        <span class="logo">👻</span>
        <h1 class="app-title">GhostChat</h1>
        <div class="input-group">
            <input type="text" id="roomInput" class="room-input" placeholder="Nhập mã phòng" maxlength="16" autocomplete="off">
            <button id="joinBtn" class="join-btn">Join Chat</button>
        </div>
        <div id="loginError" class="error-message"></div>
    </div>
</div>
<div id="chatScreen" class="screen">
    <div class="chat-container">
        <div class="chat-header">
            <div><span style="color:#8a8a8a">Phòng:</span> <span id="roomCodeDisplay" class="room-code">------</span></div>
            <button id="logoutBtn" class="logout-btn">✕</button>
        </div>
        <div id="messagesContainer" class="messages-container">
            <div id="messagesList" class="messages-list"></div>
            <div id="typingIndicator" class="typing-indicator">Đang nhập...</div>
        </div>
        <div class="input-area">
            <div class="input-wrapper">
                <button id="fileBtn" class="file-btn">+</button>
                <input type="text" id="messageInput" class="message-input" placeholder="Nhập tin nhắn...">
                <button id="sendBtn" class="send-btn">➤</button>
            </div>
            <input type="file" id="fileInput" class="file-input" multiple>
            <div id="uploadProgress" class="upload-progress">
                <div class="progress-bar"><div id="progressFill" class="progress-fill"></div></div>
                <span id="progressText" class="progress-text">0%</span>
            </div>
        </div>
    </div>
</div>
<script>
(function(){
const loginScreen=document.getElementById('loginScreen');
const chatScreen=document.getElementById('chatScreen');
const roomInput=document.getElementById('roomInput');
const joinBtn=document.getElementById('joinBtn');
const loginError=document.getElementById('loginError');
const roomCodeDisplay=document.getElementById('roomCodeDisplay');
const messagesList=document.getElementById('messagesList');
const messagesContainer=document.getElementById('messagesContainer');
const messageInput=document.getElementById('messageInput');
const sendBtn=document.getElementById('sendBtn');
const fileBtn=document.getElementById('fileBtn');
const fileInput=document.getElementById('fileInput');
const logoutBtn=document.getElementById('logoutBtn');
const typingIndicator=document.getElementById('typingIndicator');
const uploadProgress=document.getElementById('uploadProgress');
const progressFill=document.getElementById('progressFill');
const progressText=document.getElementById('progressText');

let ws=null,roomCode=null,username=null,isConnected=false,reconnectAttempts=0;
const MAX_RECONNECT=5;
let messageBuffer=[],typingTimeout=null,isTyping=false;

function formatFileSize(b){if(b===0)return'0 B';const k=1024,s=['B','KB','MB','GB'];const i=Math.floor(Math.log(b)/Math.log(k));return parseFloat((b/Math.pow(k,i)).toFixed(2))+' '+s[i];}
function getFileType(f){const e=f.split('.').pop().toLowerCase();const img=['jpg','jpeg','png','gif','bmp','webp','svg','ico','tiff'];const vid=['mp4','webm','ogg','mov','avi','mkv','flv','wmv'];const aud=['mp3','wav','ogg','aac','flac'];const doc=['pdf','doc','docx','txt','rtf','odt','xls','xlsx'];const code=['js','py','java','cpp','c','html','css','php','rb','go'];const arc=['zip','rar','7z','tar','gz'];if(img.includes(e))return'image';if(vid.includes(e))return'video';if(aud.includes(e))return'audio';if(doc.includes(e))return'document';if(code.includes(e))return'code';if(arc.includes(e))return'archive';if(e==='apk')return'apk';return'other';}
function getFileIcon(t){const icons={image:'🖼️',video:'🎬',audio:'🎵',document:'📄',code:'💻',archive:'📦',apk:'📱',other:'📎'};return icons[t]||'📎';}
function getTimestamp(){const n=new Date();return String(n.getHours()).padStart(2,'0')+':'+String(n.getMinutes()).padStart(2,'0');}
function generateUsername(){const a=['Ẩn','Bí','Mơ','Nhạt','Sâu','Vô','Hư','Ảo','Mờ','Xa'];const b=['Hồn','Mộng','Khói','Sương','Mây','Gió','Lửa','Đá','Thép','Gương'];return a[Math.floor(Math.random()*a.length)]+b[Math.floor(Math.random()*b.length)]+Math.floor(Math.random()*1000);}
function escapeHtml(t){const d=document.createElement('div');d.textContent=t;return d.innerHTML;}
function showError(m){loginError.textContent=m;loginError.classList.add('show');setTimeout(()=>loginError.classList.remove('show'),5000);}
function setLoading(l){if(l){joinBtn.disabled=true;joinBtn.innerHTML='<span class="loading-spinner"></span>';}else{joinBtn.disabled=false;joinBtn.textContent='Join Chat';}}
function scrollToBottom(){setTimeout(()=>messagesContainer.scrollTop=messagesContainer.scrollHeight,50);}
function showTyping(s){s?typingIndicator.classList.add('show'):typingIndicator.classList.remove('show');}
function updateProgress(p){if(p>=100){uploadProgress.classList.remove('show');progressFill.style.width='0%';progressText.textContent='0%';return;}uploadProgress.classList.add('show');progressFill.style.width=p+'%';progressText.textContent=p+'%';}

function renderMessage(msg){
const div=document.createElement('div');
if(msg.type==='system'){div.className='message system';div.textContent=msg.content;return div;}
const isSelf=msg.username===username;
div.className='message '+(isSelf?'self':'other');
if(msg.type==='text'){
div.innerHTML=(!isSelf?'<div class="username">'+escapeHtml(msg.username)+'</div>':'')+'<div class="content">'+escapeHtml(msg.content)+'</div><span class="timestamp">'+(msg.timestamp||getTimestamp())+'</span>';
}else if(msg.type==='file'){
const ft=getFileType(msg.file_name);const fi=getFileIcon(ft);const isImage=ft==='image';const isVideo=ft==='video';
let p='';if(isImage&&msg.file_url)p='<img src="'+msg.file_url+'" class="file-preview image" loading="lazy">';else if(isVideo&&msg.file_url)p='<video class="file-preview video" controls><source src="'+msg.file_url+'"></video>';
div.innerHTML=(!isSelf?'<div class="username">'+escapeHtml(msg.username)+'</div>':'')+'<div class="file-container" data-url="'+(msg.file_url||'')+'">'+p+'<div class="file-info"><span>'+fi+'</span><span>'+escapeHtml(msg.file_name)+'</span><span>'+formatFileSize(msg.file_size||0)+'</span></div></div><span class="timestamp">'+(msg.timestamp||getTimestamp())+'</span>';
const c=div.querySelector('.file-container');if(c&&msg.file_url)c.onclick=()=>window.open(msg.file_url,'_blank');
}
return div;
}
function appendMessage(msg){messagesList.appendChild(renderMessage(msg));scrollToBottom();}
function loadHistory(msgs){messagesList.innerHTML='';if(msgs&&msgs.length>0)msgs.forEach(m=>appendMessage(m));else{const e=document.createElement('div');e.style.cssText='text-align:center;color:#5a5a5a;padding:40px 20px;font-size:16px;';e.textContent='👻 Chưa có tin nhắn nào.';messagesList.appendChild(e);}scrollToBottom();}

function connectWebSocket(){
if(ws&&ws.readyState===WebSocket.OPEN)return;
const protocol=window.location.protocol==='https:'?'wss:':'ws:';
const url=protocol+'//'+window.location.host+'/ws/'+roomCode+'?username='+encodeURIComponent(username);
try{ws=new WebSocket(url);ws.onopen=onOpen;ws.onmessage=onMessage;ws.onclose=onClose;ws.onerror=onError;}catch(e){console.error('WS error:',e);showError('Không thể kết nối đến server');}
}
function onOpen(){isConnected=true;reconnectAttempts=0;sendBtn.disabled=false;messageInput.disabled=false;messageInput.focus();showTyping(false);if(messageBuffer.length>0){const buf=[...messageBuffer];messageBuffer=[];buf.forEach(m=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(m));});}}
function onMessage(e){try{const data=JSON.parse(e.data);if(data.type==='history')loadHistory(data.data||[]);else if(data.type==='text'||data.type==='file'||data.type==='system')appendMessage(data);else if(data.type==='typing'){data.is_typing?showTyping(true):showTyping(false);}}catch(err){console.error('Parse error:',err);}}
function onClose(e){isConnected=false;sendBtn.disabled=true;messageInput.disabled=true;showTyping(false);if(e.code!==1000&&e.code!==1001){if(reconnectAttempts<MAX_RECONNECT){reconnectAttempts++;const delay=Math.min(1000*Math.pow(2,reconnectAttempts-1),10000);setTimeout(()=>{if(roomCode&&username&&chatScreen.classList.contains('active'))connectWebSocket();},delay);}else showError('Mất kết nối server.');}}
function onError(e){console.error('WS error:',e);}
function sendMessage(type,data){const msg={type,...data};if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg));else{messageBuffer.push(msg);if(!ws||ws.readyState===WebSocket.CLOSED)connectWebSocket();}}
function sendText(c){if(!c||!c.trim())return;sendMessage('text',{content:c.trim()});messageInput.value='';messageInput.focus();}
function sendFile(url,name,size){const ft=getFileType(name);sendMessage('file',{file_url:url,file_name:name,file_size:size,file_type:ft});}
function sendTyping(t){sendMessage('typing',{is_typing:t});}

async function uploadFiles(files){
if(!files||files.length===0)return;const fd=new FormData();for(let f of files)fd.append('file',f);updateProgress(0);
try{const xhr=new XMLHttpRequest();const p=new Promise((resolve,reject)=>{xhr.open('POST','/upload/'+roomCode,true);xhr.upload.onprogress=(e)=>{if(e.lengthComputable){const percent=Math.round((e.loaded/e.total)*100);updateProgress(percent);}};xhr.onload=()=>{if(xhr.status===200){try{resolve(JSON.parse(xhr.responseText));}catch(err){reject(new Error('Invalid response'));}}else{try{const err=JSON.parse(xhr.responseText);reject(new Error(err.detail||'Upload failed'));}catch(err){reject(new Error('Upload failed: '+xhr.status));}}};xhr.onerror=()=>reject(new Error('Network error'));xhr.send(fd);});const result=await p;updateProgress(100);setTimeout(()=>updateProgress(0),1000);if(result.uploaded)result.uploaded.forEach(f=>sendFile(f.file_url,f.original_name,f.file_size));if(result.errors&&result.errors.length>0)result.errors.forEach(err=>console.warn('Upload error:',err));}catch(err){console.error('Upload error:',err);updateProgress(0);showError('Upload thất bại: '+err.message);}
}

async function joinRoom(){
const code=roomInput.value.trim().toUpperCase();
if(!code){showError('Vui lòng nhập mã phòng');return;}
if(code.length!==16){showError('Mã phòng phải có đúng 16 ký tự');return;}
setLoading(true);
try{
const res=await fetch('/check_room',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({room_key:code})});
const data=await res.json();
if(data.success){
roomCode=code;username=generateUsername();roomCodeDisplay.textContent=code;
loginScreen.classList.remove('active');chatScreen.classList.add('active');
connectWebSocket();setTimeout(()=>messageInput.focus(),300);
}else{showError(data.message||'Mã phòng không hợp lệ');roomInput.value='';roomInput.focus();}
}catch(err){console.error('Join error:',err);showError('Không thể kết nối đến server');}finally{setLoading(false);}
}

function leaveRoom(){
if(ws){try{ws.close(1000,'User left');}catch(e){}}ws=null;isConnected=false;messageBuffer=[];messagesList.innerHTML='';messageInput.value='';updateProgress(0);chatScreen.classList.remove('active');loginScreen.classList.add('active');roomInput.value='';roomInput.focus();roomCode=null;username=null;reconnectAttempts=0;sendBtn.disabled=true;messageInput.disabled=true;
}

joinBtn.onclick=joinRoom;
roomInput.onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();joinRoom();}};
sendBtn.onclick=()=>sendText(messageInput.value);
messageInput.onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendText(messageInput.value);}};
messageInput.oninput=function(){this.style.height='auto';this.style.height=Math.min(this.scrollHeight,120)+'px';if(this.value.length>0){if(!isTyping){isTyping=true;sendTyping(true);}clearTimeout(typingTimeout);typingTimeout=setTimeout(()=>{isTyping=false;sendTyping(false);},2000);}else{if(isTyping){isTyping=false;sendTyping(false);}clearTimeout(typingTimeout);}};
fileBtn.onclick=()=>fileInput.click();
fileInput.onchange=async e=>{const files=e.target.files;if(files&&files.length>0){await uploadFiles(files);fileInput.value='';}};
logoutBtn.onclick=()=>{if(confirm('Rời khỏi phòng chat?'))leaveRoom();};
roomInput.focus();
document.addEventListener('visibilitychange',()=>{if(!document.hidden&&chatScreen.classList.contains('active')&&(!ws||ws.readyState!==WebSocket.OPEN))connectWebSocket();});
window.onbeforeunload=()=>{if(ws){try{ws.close(1000,'Page unload');}catch(e){}}};
console.log('👻 GhostChat initialized');
})();
</script>
</body>
</html>"""

# ===================================================
# Utility Functions
# ===================================================

def get_timestamp() -> str:
    return datetime.now().isoformat()

def get_time_display() -> str:
    return datetime.now().strftime("%H:%M")

def sanitize_filename(filename: str) -> str:
    basename = os.path.basename(filename)
    return re.sub(r'[^a-zA-Z0-9._-]', '_', basename)

def format_file_size(size: int) -> str:
    if size == 0:
        return "0 B"
    k = 1024
    sizes = ["B", "KB", "MB", "GB"]
    i = int(min(len(sizes) - 1, (len(str(size)) - 1) // 3))
    value = size / (k ** i)
    return f"{value:.2f} {sizes[i]}"

def get_file_extension(filename: str) -> str:
    return os.path.splitext(filename)[1].lower()

def get_file_type(filename: str) -> str:
    ext = get_file_extension(filename)
    image_ext = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico', '.tiff'}
    video_ext = {'.mp4', '.webm', '.ogg', '.mov', '.avi', '.mkv', '.flv', '.wmv', '.m4v'}
    audio_ext = {'.mp3', '.wav', '.ogg', '.aac', '.flac', '.m4a'}
    doc_ext = {'.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt', '.xls', '.xlsx', '.ppt', '.pptx'}
    code_ext = {'.js', '.py', '.java', '.cpp', '.c', '.html', '.css', '.php', '.rb', '.go', '.rs', '.swift', '.kt', '.ts'}
    archive_ext = {'.zip', '.rar', '.7z', '.tar', '.gz', '.bz2', '.xz'}

    if ext in image_ext:
        return "image"
    elif ext in video_ext:
        return "video"
    elif ext in audio_ext:
        return "audio"
    elif ext in doc_ext:
        return "document"
    elif ext in code_ext:
        return "code"
    elif ext in archive_ext:
        return "archive"
    elif ext == '.apk':
        return "apk"
    else:
        return "other"

def find_free_port(start_port: int = DEFAULT_PORT, max_attempts: int = 10) -> int:
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("0.0.0.0", port))
                return port
        except OSError:
            continue
    raise RuntimeError(f"Không tìm thấy cổng trống")

# ===================================================
# API Endpoints - ĐÃ SỬA LỖI SO SÁNH
# ===================================================

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content=HTML_TEMPLATE)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "room_key": ROOM_KEY,
        "active_clients": len(CLIENTS),
        "messages_count": len(MESSAGES)
    }

@app.post("/check_room")
async def check_room(request: Request):
    try:
        data = await request.json()
        # QUAN TRỌNG: .strip() để xóa khoảng trắng, .upper() để chuyển hoa
        code = data.get("room_key", "").strip().upper()
        
        # Log để debug
        print(f"[DEBUG] Received: '{code}'")
        print(f"[DEBUG] ROOM_KEY: '{ROOM_KEY}'")
        print(f"[DEBUG] Match: {code == ROOM_KEY}")
        
        if code == ROOM_KEY:
            return JSONResponse({"success": True})
        else:
            return JSONResponse({"success": False, "message": "Invalid Room Key"})
    except Exception as e:
        print(f"[ERROR] {e}")
        return JSONResponse({"success": False, "message": "Invalid Request"})

@app.post("/upload/{room_key}")
async def upload_file(room_key: str, files: List[UploadFile] = File(...)):
    if room_key != ROOM_KEY:
        raise HTTPException(status_code=404, detail="Room not found")
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    
    uploaded_files = []
    errors = []
    
    for file in files:
        try:
            original_name = sanitize_filename(file.filename)
            if not original_name:
                continue
            content = await file.read()
            file_size = len(content)
            if file_size > MAX_FILE_SIZE:
                errors.append({
                    "filename": original_name,
                    "error": f"File exceeds maximum size of {format_file_size(MAX_FILE_SIZE)}"
                })
                continue
            ext = get_file_extension(original_name)
            unique_name = f"{uuid.uuid4().hex}{ext}"
            file_path = FILES_DIR / unique_name
            with open(file_path, "wb") as f:
                f.write(content)
            actual_size = file_path.stat().st_size
            file_url = f"/files/{room_key}/{unique_name}"
            uploaded_files.append({
                "file_url": file_url,
                "original_name": original_name,
                "file_size": actual_size,
                "new_name": unique_name,
                "file_type": get_file_type(original_name)
            })
        except Exception as e:
            errors.append({
                "filename": file.filename,
                "error": str(e)
            })
    
    if errors and not uploaded_files:
        raise HTTPException(status_code=400, detail=errors[0]["error"])
    
    return JSONResponse({
        "uploaded": uploaded_files,
        "errors": errors if errors else None
    })

@app.get("/files/{room_key}/{filename}")
async def get_file(room_key: str, filename: str):
    if room_key != ROOM_KEY:
        raise HTTPException(status_code=404, detail="Room not found")
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    file_path = FILES_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    content_type, _ = mimetypes.guess_type(filename)
    if not content_type:
        content_type = "application/octet-stream"
    return FileResponse(file_path, media_type=content_type)

# ===================================================
# WebSocket Endpoint - ĐÃ SỬA LỖI SO SÁNH
# ===================================================

@app.websocket("/ws/{room_key}")
async def websocket_endpoint(websocket: WebSocket, room_key: str):
    global MESSAGES, CLIENTS
    
    # QUAN TRỌNG: .strip() để xóa khoảng trắng
    room_key = room_key.strip()
    
    print(f"[DEBUG] WebSocket room_key: '{room_key}'")
    print(f"[DEBUG] ROOM_KEY: '{ROOM_KEY}'")
    
    if room_key != ROOM_KEY:
        print(f"[ERROR] Invalid room key: '{room_key}' != '{ROOM_KEY}'")
        await websocket.close(code=1008, reason="Invalid room key")
        return
    
    username = websocket.query_params.get("username", "Anonymous")
    await websocket.accept()
    CLIENTS.add(websocket)
    try:
        history_data = {
            "type": "history",
            "data": MESSAGES.copy()
        }
        await websocket.send_text(json.dumps(history_data))
        join_msg = {
            "type": "system",
            "content": f"{username} đã tham gia phòng",
            "timestamp": get_timestamp()
        }
        MESSAGES.append(join_msg)
        await broadcast(join_msg)
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                msg_type = message.get("type")
                if msg_type == "text":
                    content = message.get("content", "").strip()
                    if content:
                        chat_msg = {
                            "type": "text",
                            "username": username,
                            "content": content,
                            "timestamp": get_time_display()
                        }
                        MESSAGES.append(chat_msg)
                        await broadcast(chat_msg)
                elif msg_type == "file":
                    file_url = message.get("file_url")
                    file_name = message.get("file_name", "Unknown")
                    file_size = message.get("file_size", 0)
                    file_type = message.get("file_type", "other")
                    if file_url:
                        chat_msg = {
                            "type": "file",
                            "username": username,
                            "file_url": file_url,
                            "file_name": file_name,
                            "file_size": file_size,
                            "file_type": file_type,
                            "timestamp": get_time_display()
                        }
                        MESSAGES.append(chat_msg)
                        await broadcast(chat_msg)
                elif msg_type == "typing":
                    typing_msg = {
                        "type": "typing",
                        "username": username,
                        "is_typing": message.get("is_typing", False)
                    }
                    await broadcast_to_others(websocket, typing_msg)
            except json.JSONDecodeError:
                pass
            except Exception as e:
                print(f"Error processing message: {e}")
    except WebSocketDisconnect:
        CLIENTS.discard(websocket)
        leave_msg = {
            "type": "system",
            "content": f"{username} đã rời phòng",
            "timestamp": get_timestamp()
        }
        MESSAGES.append(leave_msg)
        await broadcast(leave_msg)
    except Exception as e:
        CLIENTS.discard(websocket)
        print(f"WebSocket error: {e}")

# ===================================================
# Broadcasting
# ===================================================

async def broadcast(message: dict) -> None:
    if not CLIENTS:
        return
    data = json.dumps(message)
    disconnected = set()
    for client in CLIENTS:
        try:
            await client.send_text(data)
        except Exception:
            disconnected.add(client)
    for client in disconnected:
        CLIENTS.discard(client)

async def broadcast_to_others(sender: WebSocket, message: dict) -> None:
    if not CLIENTS:
        return
    data = json.dumps(message)
    disconnected = set()
    for client in CLIENTS:
        if client == sender:
            continue
        try:
            await client.send_text(data)
        except Exception:
            disconnected.add(client)
    for client in disconnected:
        CLIENTS.discard(client)

# ===================================================
# Cleanup
# ===================================================

async def cleanup() -> None:
    global SHUTDOWN_DONE
    if SHUTDOWN_DONE:
        return
    SHUTDOWN_DONE = True

    if CLIENTS:
        for client in CLIENTS:
            try:
                await client.close(code=1001, reason="Server shutting down")
            except Exception:
                pass
        CLIENTS.clear()
    MESSAGES.clear()

    room_dir = TEMP_DIR / ROOM_KEY
    if room_dir.exists():
        try:
            shutil.rmtree(room_dir)
        except Exception:
            pass

# ===================================================
# Main
# ===================================================

if __name__ == "__main__":
    import os
    
    port = int(os.environ.get("PORT", 8000))
    
    print("="*50)
    print("👻 GhostChat")
    print("="*50)
    print(f"Server running on http://localhost:{port}")
    print(f"Room Key: {ROOM_KEY}")
    print("="*50)
    
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="critical", access_log=False)
