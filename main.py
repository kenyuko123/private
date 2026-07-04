#!/usr/bin/env python3
"""
GhostChat - Chat ứng dụng realtime với WebSocket
Hỗ trợ upload tất cả định dạng file, xem ảnh/video full màn hình, download file
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
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB
ROOM_CODE_LENGTH = 16
TEMP_DIR = Path("temp")
MAX_HISTORY = 100

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
# HTML Template - ĐÃ SỬA LỖI UPLOAD
# ===================================================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>GhostChat</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>👻</text></svg>">
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Oxygen,Ubuntu,sans-serif;background:#0a0a0a;color:#e4e6eb;height:100vh;overflow:hidden}
        
        .screen{display:none;width:100%;height:100vh;position:fixed;top:0;left:0;background:#0a0a0a;z-index:1}
        .screen.active{display:flex;justify-content:center;align-items:center}
        
        .login-container{max-width:420px;width:90%;padding:40px 30px;background:#1a1a1a;border-radius:20px;text-align:center;animation:fadeIn .5s ease}
        @keyframes fadeIn{from{opacity:0;transform:translateY(30px)}to{opacity:1;transform:translateY(0)}}
        .logo{font-size:72px;display:block;margin-bottom:10px;animation:float 3s ease-in-out infinite}
        @keyframes float{0%,100%{transform:translateY(0)}50%{transform:translateY(-10px)}}
        .app-title{font-size:36px;font-weight:700;background:linear-gradient(135deg,#667eea,#764ba2);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:30px}
        .room-input{width:100%;padding:16px;background:#2a2a2a;border:2px solid #3a3a3a;border-radius:12px;color:#e4e6eb;font-size:18px;text-align:center;outline:none;transition:all .3s}
        .room-input:focus{border-color:#667eea;box-shadow:0 0 20px rgba(102,126,234,.15)}
        .join-btn{width:100%;padding:16px;margin-top:12px;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border:none;border-radius:12px;font-size:18px;font-weight:600;cursor:pointer;transition:all .3s}
        .join-btn:hover{transform:translateY(-2px);box-shadow:0 8px 25px rgba(102,126,234,.3)}
        .join-btn:disabled{opacity:.6;cursor:not-allowed;transform:none}
        .error-message{color:#ff6b6b;font-size:14px;margin-top:10px;display:none;padding:8px;background:rgba(255,107,107,.1);border-radius:8px;border-left:3px solid #ff6b6b}
        .error-message.show{display:block;animation:shake .4s ease}
        @keyframes shake{0%,100%{transform:translateX(0)}25%{transform:translateX(-10px)}75%{transform:translateX(10px)}}
        
        .chat-container{width:100%;height:100vh;max-width:900px;margin:0 auto;display:flex;flex-direction:column;background:#0f0f0f}
        .chat-header{display:flex;justify-content:space-between;align-items:center;padding:12px 20px;background:#1a1a1a;border-bottom:1px solid #2a2a2a;flex-shrink:0;min-height:60px}
        .room-info{display:flex;align-items:center;gap:8px;font-size:14px}
        .room-info .label{color:#8a8a8a}
        .room-code{color:#667eea;font-weight:600;background:#2a2a2a;padding:4px 12px;border-radius:6px;font-family:monospace;font-size:14px}
        .header-actions{display:flex;gap:8px;align-items:center}
        .logout-btn{width:36px;height:36px;background:#2a2a2a;border:none;border-radius:50%;color:#8a8a8a;font-size:20px;cursor:pointer;transition:all .2s;display:flex;align-items:center;justify-content:center}
        .logout-btn:hover{background:#3a2a2a;color:#ff6b6b;transform:rotate(90deg)}
        
        .messages-container{flex:1;overflow-y:auto;padding:16px 20px;background:#0f0f0f;scroll-behavior:smooth}
        .messages-container::-webkit-scrollbar{width:6px}
        .messages-container::-webkit-scrollbar-track{background:#1a1a1a}
        .messages-container::-webkit-scrollbar-thumb{background:#3a3a3a;border-radius:3px}
        .messages-container::-webkit-scrollbar-thumb:hover{background:#4a4a4a}
        .messages-list{display:flex;flex-direction:column;gap:6px;min-height:100%}
        
        .message{max-width:80%;padding:8px 14px;border-radius:18px;animation:slideIn .3s ease;position:relative;word-wrap:break-word}
        @keyframes slideIn{from{opacity:0;transform:translateY(10px) scale(.96)}to{opacity:1;transform:translateY(0) scale(1)}}
        .message.self{align-self:flex-end;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border-bottom-right-radius:4px}
        .message.other{align-self:flex-start;background:#1e1e1e;border-bottom-left-radius:4px}
        .message.system{align-self:center;background:#1a1a1a;color:#8a8a8a;font-size:13px;padding:4px 16px;border-radius:20px;max-width:90%}
        .message .username{font-size:12px;font-weight:600;color:#667eea;margin-bottom:2px}
        .message.other .username{color:#8a8a8a}
        .message .content{font-size:15px;line-height:1.4;word-break:break-word}
        .message .timestamp{font-size:10px;opacity:.5;margin-top:3px;text-align:right;display:block}
        
        .message .file-wrapper{background:rgba(0,0,0,.2);border-radius:12px;padding:10px;margin-top:2px;position:relative}
        .message.self .file-wrapper{background:rgba(0,0,0,.25)}
        .message .file-preview{max-width:100%;border-radius:8px;display:block;margin-bottom:6px;cursor:pointer}
        .message .file-preview.image{max-height:350px;width:auto;object-fit:contain}
        .message .file-preview.video{max-height:350px;width:100%;object-fit:contain;border-radius:8px}
        .message .file-info{display:flex;align-items:center;gap:8px;font-size:13px;padding:4px 0;flex-wrap:wrap}
        .message .file-icon{font-size:20px}
        .message .file-name{font-weight:500;flex:1;word-break:break-all;font-size:13px}
        .message .file-size{opacity:.6;font-size:11px;white-space:nowrap}
        
        .file-actions{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-top:4px}
        .file-actions .btn{background:rgba(255,255,255,.1);border:none;padding:4px 10px;border-radius:6px;cursor:pointer;font-size:12px;transition:all .2s;display:inline-flex;align-items:center;gap:4px;color:#e4e6eb}
        .file-actions .btn:hover{background:rgba(255,255,255,.2)}
        .message.self .file-actions .btn{background:rgba(255,255,255,.15)}
        .message.self .file-actions .btn:hover{background:rgba(255,255,255,.25)}
        .file-actions .btn-download{color:#667eea}
        .file-actions .btn-download:hover{background:rgba(102,126,234,.2)}
        
        .viewer-overlay{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.95);z-index:9999;justify-content:center;align-items:center;flex-direction:column}
        .viewer-overlay.active{display:flex}
        .viewer-overlay .close-btn{position:absolute;top:20px;right:20px;background:rgba(255,255,255,.1);border:none;color:#fff;font-size:30px;cursor:pointer;padding:10px 18px;border-radius:50%;transition:all .3s;z-index:10000}
        .viewer-overlay .close-btn:hover{background:rgba(255,255,255,.2);transform:rotate(90deg)}
        .viewer-overlay .download-btn-top{position:absolute;top:20px;right:80px;background:rgba(255,255,255,.1);border:none;color:#fff;padding:10px 16px;border-radius:8px;cursor:pointer;font-size:16px;transition:all .3s;z-index:10000;display:flex;align-items:center;gap:8px}
        .viewer-overlay .download-btn-top:hover{background:rgba(255,255,255,.2)}
        .viewer-overlay .viewer-content{max-width:95%;max-height:90%;object-fit:contain}
        .viewer-overlay .viewer-content.image-viewer{max-width:95%;max-height:90%;object-fit:contain}
        .viewer-overlay .viewer-content.video-viewer{max-width:95%;max-height:90%;width:auto}
        .viewer-overlay .file-name-display{position:absolute;bottom:30px;color:#8a8a8a;font-size:14px;text-align:center;max-width:80%;word-break:break-all}
        
        .typing-indicator{display:none;padding:6px 16px;color:#8a8a8a;font-size:13px;font-style:italic}
        .typing-indicator.show{display:block;animation:pulse 1.5s ease-in-out infinite}
        @keyframes pulse{0%,100%{opacity:.4}50%{opacity:1}}
        
        .input-area{flex-shrink:0;padding:10px 16px 16px;background:#1a1a1a;border-top:1px solid #2a2a2a}
        .input-wrapper{display:flex;align-items:center;gap:8px;background:#2a2a2a;border-radius:25px;padding:4px 6px 4px 14px;border:2px solid #3a3a3a;transition:all .3s}
        .input-wrapper:focus-within{border-color:#667eea;box-shadow:0 0 20px rgba(102,126,234,.1)}
        .file-btn{width:38px;height:38px;border-radius:50%;border:none;background:transparent;color:#8a8a8a;font-size:22px;cursor:pointer;transition:all .2s;display:flex;align-items:center;justify-content:center;flex-shrink:0}
        .file-btn:hover{background:#3a3a3a;color:#667eea;transform:rotate(90deg)}
        .message-input{flex:1;background:transparent;border:none;outline:none;color:#e4e6eb;font-size:15px;padding:6px 0;min-height:36px;max-height:100px;resize:none;font-family:inherit}
        .message-input::placeholder{color:#6a6a6a}
        .send-btn{width:38px;height:38px;border-radius:50%;border:none;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;font-size:18px;cursor:pointer;transition:all .2s;display:flex;align-items:center;justify-content:center;flex-shrink:0}
        .send-btn:hover{transform:scale(1.05);box-shadow:0 4px 15px rgba(102,126,234,.4)}
        .send-btn:active{transform:scale(.95)}
        .send-btn:disabled{opacity:.5;cursor:not-allowed;transform:none}
        
        .upload-progress{display:none;margin-top:6px;padding:6px 12px;background:#2a2a2a;border-radius:8px;align-items:center;gap:10px}
        .upload-progress.show{display:flex}
        .progress-bar{flex:1;height:4px;background:#3a3a3a;border-radius:2px;overflow:hidden}
        .progress-fill{height:100%;background:linear-gradient(135deg,#667eea,#764ba2);width:0;transition:width .3s}
        .progress-text{color:#8a8a8a;font-size:12px;min-width:36px;text-align:right}
        .file-input{display:none}
        
        .loading-spinner{display:inline-block;width:20px;height:20px;border:3px solid rgba(102,126,234,.2);border-top-color:#667eea;border-radius:50%;animation:spin .6s linear infinite}
        @keyframes spin{to{transform:rotate(360deg)}}
        
        @media(max-width:768px){
            .login-container{padding:30px 20px}
            .logo{font-size:60px}
            .app-title{font-size:28px}
            .message{max-width:85%;padding:6px 12px}
            .message .content{font-size:14px}
            .chat-header{padding:10px 16px}
            .room-code{font-size:12px;padding:3px 10px}
            .messages-container{padding:12px 16px}
            .input-area{padding:8px 12px 14px}
            .input-wrapper{padding:3px 5px 3px 12px}
            .message .file-preview.image{max-height:250px}
            .message .file-preview.video{max-height:250px}
            .room-info .label{font-size:12px}
            .viewer-overlay .close-btn{top:10px;right:10px;font-size:24px;padding:8px 14px}
            .viewer-overlay .download-btn-top{top:10px;right:60px;padding:8px 12px;font-size:14px}
        }
        @media(max-width:480px){
            .login-container{padding:20px 16px}
            .logo{font-size:48px}
            .app-title{font-size:22px}
            .room-input{font-size:16px;padding:14px}
            .message{max-width:92%;padding:6px 10px;font-size:13px}
            .message .content{font-size:13px}
            .message .file-preview.image{max-height:200px}
            .message .file-preview.video{max-height:200px}
            .message .file-info{font-size:12px}
            .file-btn{width:34px;height:34px;font-size:20px}
            .send-btn{width:34px;height:34px;font-size:16px}
            .message-input{font-size:14px}
            .room-code{font-size:11px;padding:2px 8px}
            .room-info .label{font-size:11px}
            .logout-btn{width:32px;height:32px;font-size:18px}
            .chat-header{padding:8px 12px;min-height:50px}
            .messages-container{padding:10px 12px}
            .input-area{padding:6px 10px 12px}
            .input-wrapper{padding:2px 4px 2px 10px;border-radius:20px}
            .viewer-overlay .close-btn{top:10px;right:10px;font-size:20px;padding:6px 12px}
            .viewer-overlay .download-btn-top{top:10px;right:55px;padding:6px 10px;font-size:12px}
        }
        .messages-container{scrollbar-width:thin;scrollbar-color:#3a3a3a #1a1a1a}
        ::selection{background:#667eea;color:#fff}
    </style>
</head>
<body>
<div id="loginScreen" class="screen active">
    <div class="login-container">
        <span class="logo">👻</span>
        <h1 class="app-title">GhostChat</h1>
        <div class="input-group">
            <input type="text" id="roomInput" class="room-input" placeholder="Nhập mã phòng" maxlength="16" autocomplete="off" spellcheck="false">
            <button id="joinBtn" class="join-btn">Join Chat</button>
        </div>
        <div id="loginError" class="error-message"></div>
    </div>
</div>

<div id="chatScreen" class="screen">
    <div class="chat-container">
        <div class="chat-header">
            <div class="room-info">
                <span class="label">📌 Phòng:</span>
                <span id="roomCodeDisplay" class="room-code">------</span>
            </div>
            <div class="header-actions">
                <button id="logoutBtn" class="logout-btn" title="Rời phòng">✕</button>
            </div>
        </div>
        <div id="messagesContainer" class="messages-container">
            <div id="messagesList" class="messages-list"></div>
            <div id="typingIndicator" class="typing-indicator">👤 Đang nhập...</div>
        </div>
        <div class="input-area">
            <div class="input-wrapper">
                <button id="fileBtn" class="file-btn" title="Đính kèm file">➕</button>
                <input type="text" id="messageInput" class="message-input" placeholder="Nhập tin nhắn..." autocomplete="off">
                <button id="sendBtn" class="send-btn" title="Gửi">➤</button>
            </div>
            <input type="file" id="fileInput" class="file-input" multiple>
            <div id="uploadProgress" class="upload-progress">
                <div class="progress-bar"><div id="progressFill" class="progress-fill"></div></div>
                <span id="progressText" class="progress-text">0%</span>
            </div>
        </div>
    </div>
</div>

<div id="viewerOverlay" class="viewer-overlay">
    <button class="close-btn" onclick="closeViewer()">✕</button>
    <button class="download-btn-top" onclick="downloadViewerFile()">⬇️ Tải xuống</button>
    <div id="viewerContent"></div>
    <div id="viewerFileName" class="file-name-display"></div>
</div>

<script>
(function(){
'use strict';

// DOM Elements
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
const viewerOverlay=document.getElementById('viewerOverlay');
const viewerContent=document.getElementById('viewerContent');
const viewerFileName=document.getElementById('viewerFileName');

let ws=null,roomCode=null,username=null,isConnected=false,reconnectAttempts=0;
const MAX_RECONNECT=5;
let messageBuffer=[],typingTimeout=null,isTyping=false;
let currentViewerUrl='',currentViewerName='';

// ===== Utilities =====
function formatFileSize(b){if(b===0)return'0 B';const k=1024,s=['B','KB','MB','GB'];const i=Math.floor(Math.log(b)/Math.log(k));return parseFloat((b/Math.pow(k,i)).toFixed(2))+' '+s[i];}
function getFileType(f){const e=f.split('.').pop().toLowerCase();const img=['jpg','jpeg','png','gif','bmp','webp','svg','ico','tiff'];const vid=['mp4','webm','ogg','mov','avi','mkv','flv','wmv','m4v'];const aud=['mp3','wav','ogg','aac','flac','m4a'];const doc=['pdf','doc','docx','txt','rtf','odt','xls','xlsx','ppt','pptx'];const code=['js','py','java','cpp','c','html','css','php','rb','go','rs','swift','kt','ts'];const arc=['zip','rar','7z','tar','gz','bz2','xz'];if(img.includes(e))return'image';if(vid.includes(e))return'video';if(aud.includes(e))return'audio';if(doc.includes(e))return'document';if(code.includes(e))return'code';if(arc.includes(e))return'archive';if(e==='apk')return'apk';return'other';}
function getFileIcon(t){const icons={image:'🖼️',video:'🎬',audio:'🎵',document:'📄',code:'💻',archive:'📦',apk:'📱',other:'📎'};return icons[t]||'📎';}
function getTimestamp(){const n=new Date();return String(n.getHours()).padStart(2,'0')+':'+String(n.getMinutes()).padStart(2,'0');}
function generateUsername(){const a=['Ẩn','Bí','Mơ','Nhạt','Sâu','Vô','Hư','Ảo','Mờ','Xa'];const b=['Hồn','Mộng','Khói','Sương','Mây','Gió','Lửa','Đá','Thép','Gương'];return a[Math.floor(Math.random()*a.length)]+b[Math.floor(Math.random()*b.length)]+Math.floor(Math.random()*1000);}
function escapeHtml(t){const d=document.createElement('div');d.textContent=t;return d.innerHTML;}

function showError(m){loginError.textContent=m;loginError.classList.add('show');setTimeout(()=>loginError.classList.remove('show'),5000);}
function setLoading(l){if(l){joinBtn.disabled=true;joinBtn.innerHTML='<span class="loading-spinner"></span>';}else{joinBtn.disabled=false;joinBtn.textContent='Join Chat';}}
function scrollToBottom(){setTimeout(()=>messagesContainer.scrollTop=messagesContainer.scrollHeight,50);}
function showTyping(s){s?typingIndicator.classList.add('show'):typingIndicator.classList.remove('show');}
function updateProgress(p){if(p>=100){uploadProgress.classList.remove('show');progressFill.style.width='0%';progressText.textContent='0%';return;}uploadProgress.classList.add('show');progressFill.style.width=p+'%';progressText.textContent=p+'%';}

// ===== Fullscreen Viewer =====
window.openViewer=function(url,name,type){
    currentViewerUrl=url;
    currentViewerName=name;
    viewerFileName.textContent=name;
    if(type==='image'){
        viewerContent.innerHTML='<img src="'+url+'" class="viewer-content image-viewer" alt="'+escapeHtml(name)+'">';
    }else if(type==='video'){
        viewerContent.innerHTML='<video class="viewer-content video-viewer" controls autoplay><source src="'+url+'"></video>';
    }else{
        viewerContent.innerHTML='<div style="color:#fff;font-size:24px;text-align:center;padding:40px">📄 '+escapeHtml(name)+'<br><br><button onclick="window.open(\\''+url+'\\',\\'_blank\\')" style="padding:12px 24px;background:#667eea;border:none;border-radius:8px;color:#fff;font-size:16px;cursor:pointer">Mở file</button></div>';
    }
    viewerOverlay.classList.add('active');
    document.body.style.overflow='hidden';
};

window.closeViewer=function(){
    viewerOverlay.classList.remove('active');
    document.body.style.overflow='';
    viewerContent.innerHTML='';
    currentViewerUrl='';
    currentViewerName='';
};

window.downloadViewerFile=function(){
    if(currentViewerUrl){
        const a=document.createElement('a');
        a.href=currentViewerUrl;
        a.download=currentViewerName||'file';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }
};

document.addEventListener('keydown',function(e){if(e.key==='Escape')closeViewer();});

// ===== Download File =====
window.downloadFile=function(url,name){if(!url)return;const a=document.createElement('a');a.href=url;a.download=name||'file';document.body.appendChild(a);a.click();document.body.removeChild(a);};

// ===== Render Message =====
function renderMessage(msg){
const div=document.createElement('div');
if(msg.type==='system'){div.className='message system';div.textContent=msg.content;return div;}
const isSelf=msg.username===username;
div.className='message '+(isSelf?'self':'other');

if(msg.type==='text'){
div.innerHTML=(!isSelf?'<div class="username">'+escapeHtml(msg.username)+'</div>':'')+'<div class="content">'+escapeHtml(msg.content)+'</div><span class="timestamp">'+(msg.timestamp||getTimestamp())+'</span>';
}else if(msg.type==='file'){
const ft=getFileType(msg.file_name);
const fi=getFileIcon(ft);
const isImage=ft==='image';
const isVideo=ft==='video';
const isViewable=isImage||isVideo;
let preview='';
if(isImage&&msg.file_url){
preview='<img src="'+msg.file_url+'" class="file-preview image" onclick="openViewer(\\''+msg.file_url+'\\',\\''+escapeHtml(msg.file_name)+'\\',\\'image\\')" loading="lazy">';
}else if(isVideo&&msg.file_url){
preview='<video class="file-preview video" onclick="openViewer(\\''+msg.file_url+'\\',\\''+escapeHtml(msg.file_name)+'\\',\\'video\\')"><source src="'+msg.file_url+'"></video>';
}

let fileHtml='<div class="file-wrapper">'+preview;
fileHtml+='<div class="file-info"><span class="file-icon">'+fi+'</span><span class="file-name">'+escapeHtml(msg.file_name)+'</span><span class="file-size">'+formatFileSize(msg.file_size||0)+'</span></div>';
fileHtml+='<div class="file-actions">';
if(isViewable&&msg.file_url){
fileHtml+='<button class="btn" onclick="openViewer(\\''+msg.file_url+'\\',\\''+escapeHtml(msg.file_name)+'\\',\\''+ft+'\\')">👁️ Xem</button>';
}
fileHtml+='<button class="btn btn-download" onclick="downloadFile(\\''+msg.file_url+'\\',\\''+escapeHtml(msg.file_name)+'\\')">⬇️ Download</button>';
fileHtml+='</div></div>';
div.innerHTML=(!isSelf?'<div class="username">'+escapeHtml(msg.username)+'</div>':'')+fileHtml+'<span class="timestamp">'+(msg.timestamp||getTimestamp())+'</span>';
}
return div;
}

function appendMessage(msg){
if(!msg)return;
const el=renderMessage(msg);
if(el)messagesList.appendChild(el);
scrollToBottom();
}

function loadHistory(msgs){
messagesList.innerHTML='';
if(msgs&&msgs.length>0){
msgs.forEach(m=>appendMessage(m));
}else{
const e=document.createElement('div');
e.style.cssText='text-align:center;color:#5a5a5a;padding:40px 20px;font-size:16px;';
e.textContent='👻 Chưa có tin nhắn nào. Hãy bắt đầu trò chuyện!';
messagesList.appendChild(e);
}
scrollToBottom();
}

// ===== WebSocket =====
function connectWebSocket(){
if(ws&&ws.readyState===WebSocket.OPEN)return;
const protocol=window.location.protocol==='https:'?'wss:':'ws:';
const url=protocol+'//'+window.location.host+'/ws/'+roomCode+'?username='+encodeURIComponent(username);
try{ws=new WebSocket(url);ws.onopen=onOpen;ws.onmessage=onMessage;ws.onclose=onClose;ws.onerror=onError;}catch(e){console.error('WS error:',e);showError('Không thể kết nối đến server');}
}
function onOpen(){isConnected=true;reconnectAttempts=0;sendBtn.disabled=false;messageInput.disabled=false;messageInput.focus();showTyping(false);if(messageBuffer.length>0){const buf=[...messageBuffer];messageBuffer=[];buf.forEach(m=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(m));});}}
function onMessage(e){try{const data=JSON.parse(e.data);if(data.type==='history')loadHistory(data.data||[]);else if(data.type==='text'||data.type==='file'||data.type==='system')appendMessage(data);else if(data.type==='typing'){data.is_typing?showTyping(true):showTyping(false);}}catch(err){console.error('Parse error:',err);}}
function onClose(e){isConnected=false;sendBtn.disabled=true;messageInput.disabled=true;showTyping(false);if(e.code!==1000&&e.code!==1001){if(reconnectAttempts<MAX_RECONNECT){reconnectAttempts++;const delay=Math.min(1000*Math.pow(2,reconnectAttempts-1),10000);setTimeout(()=>{if(roomCode&&username&&chatScreen.classList.contains('active'))connectWebSocket();},delay);}else showError('Mất kết nối server. Vui lòng tải lại trang.');}}
function onError(e){console.error('WS error:',e);}
function sendMessage(type,data){const msg={type,...data};if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg));else{messageBuffer.push(msg);if(!ws||ws.readyState===WebSocket.CLOSED)connectWebSocket();}}
function sendText(c){if(!c||!c.trim())return;sendMessage('text',{content:c.trim()});messageInput.value='';messageInput.focus();}
function sendFile(url,name,size){const ft=getFileType(name);sendMessage('file',{file_url:url,file_name:name,file_size:size,file_type:ft});}
function sendTyping(t){sendMessage('typing',{is_typing:t});}

// ===== UPLOAD FILES - ĐÃ SỬA LỖI =====
async function uploadFiles(files){
    if(!files||files.length===0)return;
    
    const formData=new FormData();
    for(let f of files){
        formData.append('file',f);
    }
    
    updateProgress(0);
    
    try{
        const xhr=new XMLHttpRequest();
        const uploadPromise=new Promise((resolve,reject)=>{
            xhr.open('POST','/upload/'+roomCode,true);
            
            xhr.upload.onprogress=(e)=>{
                if(e.lengthComputable){
                    const percent=Math.round((e.loaded/e.total)*100);
                    updateProgress(percent);
                }
            };
            
            xhr.onload=()=>{
                if(xhr.status===200){
                    try{
                        const response=JSON.parse(xhr.responseText);
                        resolve(response);
                    }catch(err){
                        reject(new Error('Invalid response'));
                    }
                }else{
                    try{
                        const error=JSON.parse(xhr.responseText);
                        reject(new Error(error.detail||'Upload failed'));
                    }catch(err){
                        reject(new Error('Upload failed with status: '+xhr.status));
                    }
                }
            };
            
            xhr.onerror=()=>{
                reject(new Error('Network error during upload'));
            };
            
            xhr.send(formData);
        });
        
        const result=await uploadPromise;
        updateProgress(100);
        setTimeout(()=>updateProgress(0),1000);
        
        // ✅ Gửi URL file qua WebSocket sau khi upload thành công
        if(result.uploaded&&result.uploaded.length>0){
            result.uploaded.forEach(file=>{
                sendFile(file.file_url,file.original_name,file.file_size);
            });
        }
        
        if(result.errors&&result.errors.length>0){
            result.errors.forEach(err=>{
                console.warn('Upload error:',err);
                showError('Upload lỗi: '+err.error);
            });
        }
    }catch(err){
        console.error('Upload error:',err);
        updateProgress(0);
        showError('Upload thất bại: '+err.message);
    }
}

// ===== Join Room =====
async function joinRoom(){
const code=roomInput.value.trim();
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

// ===== Event Listeners =====
joinBtn.onclick=joinRoom;
roomInput.onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();joinRoom();}};
sendBtn.onclick=()=>sendText(messageInput.value);
messageInput.onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendText(messageInput.value);}};
messageInput.oninput=function(){this.style.height='auto';this.style.height=Math.min(this.scrollHeight,100)+'px';if(this.value.length>0){if(!isTyping){isTyping=true;sendTyping(true);}clearTimeout(typingTimeout);typingTimeout=setTimeout(()=>{isTyping=false;sendTyping(false);},2000);}else{if(isTyping){isTyping=false;sendTyping(false);}clearTimeout(typingTimeout);}};
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
# API Endpoints
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
        code = data.get("room_key", "").strip()
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
    print(f"[UPLOAD] Room key: {room_key}")
    print(f"[UPLOAD] ROOM_KEY: {ROOM_KEY}")
    print(f"[UPLOAD] Files count: {len(files)}")
    
    if room_key != ROOM_KEY:
        print(f"[UPLOAD] Invalid room key!")
        raise HTTPException(status_code=404, detail="Room not found")
    
    if not files:
        print(f"[UPLOAD] No files!")
        raise HTTPException(status_code=400, detail="No files provided")
    
    uploaded_files = []
    errors = []
    
    for file in files:
        try:
            print(f"[UPLOAD] Processing file: {file.filename}")
            original_name = sanitize_filename(file.filename)
            if not original_name:
                continue
            
            content = await file.read()
            file_size = len(content)
            print(f"[UPLOAD] File size: {file_size} bytes")
            
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
            print(f"[UPLOAD] File saved: {file_path}, URL: {file_url}")
            
            uploaded_files.append({
                "file_url": file_url,
                "original_name": original_name,
                "file_size": actual_size,
                "new_name": unique_name,
                "file_type": get_file_type(original_name)
            })
        except Exception as e:
            print(f"[UPLOAD] Error: {e}")
            errors.append({
                "filename": file.filename,
                "error": str(e)
            })
    
    if errors and not uploaded_files:
        raise HTTPException(status_code=400, detail=errors[0]["error"])
    
    print(f"[UPLOAD] Success: {len(uploaded_files)} files, Errors: {len(errors)}")
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
# WebSocket Endpoint
# ===================================================

@app.websocket("/ws/{room_key}")
async def websocket_endpoint(websocket: WebSocket, room_key: str):
    global MESSAGES, CLIENTS
    
    room_key = room_key.strip()
    
    if room_key != ROOM_KEY:
        await websocket.close(code=1008, reason="Invalid room key")
        return
    
    username = websocket.query_params.get("username", "Anonymous")
    
    try:
        await websocket.accept()
        CLIENTS.add(websocket)
        
        history_data = {
            "type": "history",
            "data": MESSAGES[-MAX_HISTORY:] if MESSAGES else []
        }
        await websocket.send_text(json.dumps(history_data))
        
        join_msg = {
            "type": "system",
            "content": f"{username} đã tham gia phòng",
            "timestamp": get_timestamp()
        }
        MESSAGES.append(join_msg)
        if len(MESSAGES) > MAX_HISTORY:
            MESSAGES = MESSAGES[-MAX_HISTORY:]
        await broadcast(join_msg)
        
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
                try:
                    message = json.loads(data)
                    msg_type = message.get("type")
                    
                    if msg_type == "text":
                        content = message.get("content", "").strip()
                        if content and len(content) > 0:
                            chat_msg = {
                                "type": "text",
                                "username": username,
                                "content": content,
                                "timestamp": get_time_display()
                            }
                            MESSAGES.append(chat_msg)
                            if len(MESSAGES) > MAX_HISTORY:
                                MESSAGES = MESSAGES[-MAX_HISTORY:]
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
                            if len(MESSAGES) > MAX_HISTORY:
                                MESSAGES = MESSAGES[-MAX_HISTORY:]
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
            except asyncio.TimeoutError:
                continue
            except WebSocketDisconnect:
                break
            except Exception as e:
                print(f"WebSocket receive error: {e}")
                break
                
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        CLIENTS.discard(websocket)
        leave_msg = {
            "type": "system",
            "content": f"{username} đã rời phòng",
            "timestamp": get_timestamp()
        }
        MESSAGES.append(leave_msg)
        if len(MESSAGES) > MAX_HISTORY:
            MESSAGES = MESSAGES[-MAX_HISTORY:]
        await broadcast(leave_msg)

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
