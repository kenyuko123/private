#!/usr/bin/env python3
import json,time,urllib.request,urllib.error

FIREBASE_BASE='https://realtime-database-bee52-default-rtdb.asia-southeast1.firebasedatabase.app'
GITHUB_TOKEN='PASTE_GITHUB_TOKEN_HERE'
GITHUB_REPO='YOUR_USERNAME/rdp'
WORKFLOW_FILE='main.yml'
BRANCH='main'
POLL_SECONDS=2

def request(url,method='GET',headers=None,payload=None):
    headers=headers or {}; data=None
    if payload is not None:
        data=json.dumps(payload).encode(); headers={**headers,'Content-Type':'application/json'}
    req=urllib.request.Request(url,data=data,method=method,headers=headers)
    try:
        with urllib.request.urlopen(req,timeout=20) as r:
            raw=r.read().decode('utf-8','replace')
            return r.status,(json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw=e.read().decode('utf-8','replace')
        try: detail=json.loads(raw)
        except: detail=raw
        raise RuntimeError(f'HTTP {e.code}: {detail}')
    except urllib.error.URLError as e:
        raise RuntimeError(f'Network error: {e.reason}')

def firebase(path,method='GET',payload=None):
    return request(FIREBASE_BASE.rstrip('/')+'/'+path.lstrip('/'),method,
                   {'Accept':'application/json'},payload)

def github(path,method='GET',payload=None):
    if not GITHUB_TOKEN or GITHUB_TOKEN=='PASTE_GITHUB_TOKEN_HERE':
        raise RuntimeError('Chưa điền GITHUB_TOKEN trong main.py')
    h={'Accept':'application/vnd.github+json','Authorization':f'Bearer {GITHUB_TOKEN}',
       'X-GitHub-Api-Version':'2022-11-28','User-Agent':'rdp-firebase-worker/1.0'}
    return request('https://api.github.com/'+path.lstrip('/'),method,h,payload)

def dispatch():
    base=f'repos/{GITHUB_REPO}'
    github(base)
    _,wf=github(f'{base}/actions/workflows/{WORKFLOW_FILE}')
    if wf.get('state')!='active': raise RuntimeError(f'{WORKFLOW_FILE} không active')
    github(f'{base}/actions/workflows/{WORKFLOW_FILE}/dispatches','POST',{'ref':BRANCH})

def process(cmd):
    if not isinstance(cmd,dict): return
    action=str(cmd.get('action','')).strip().lower()
    rid=cmd.get('request_id','unknown')
    if action!='create':
        print('[!] Command không hợp lệ, xoá queue.')
        firebase('/rdp/command.json','DELETE'); return
    print(f'[*] CREATE request: {rid}')
    # XOÁ TRƯỚC khi dispatch để tuyệt đối không loop.
    try:
        firebase('/rdp/command.json','DELETE')
        print('[OK] Đã xoá command khỏi Firebase.')
    except Exception as e:
        print('[ERROR] Không xoá được command:',e)
        print('[!] Không dispatch để tránh duplicate.')
        return
    try:
        dispatch()
        print('[OK] main.yml đã được dispatch.')
    except Exception as e:
        print('[ERROR] Dispatch thất bại:',e)
        print('[!] Command đã xoá; chờ request mới.')

def main():
    print('==========================================')
    print('       RDP FIREBASE COMMAND WORKER')
    print('==========================================')
    print('Firebase:',FIREBASE_BASE)
    print('GitHub  :',GITHUB_REPO)
    print('Workflow:',WORKFLOW_FILE)
    print('[*] Chờ /rdp/command ... Ctrl+C để dừng.')
    last_error=None
    while True:
        try:
            _,cmd=firebase('/rdp/command.json')
            if cmd:
                process(cmd); last_error=None
            elif last_error:
                print('[OK] Firebase đã kết nối lại.'); last_error=None
        except KeyboardInterrupt:
            print('\n[OK] Đã dừng.'); return
        except Exception as e:
            msg=str(e)
            if msg!=last_error:
                print('[ERROR]',msg); print('[*] Tiếp tục thử...'); last_error=msg
        time.sleep(POLL_SECONDS)

if __name__=='__main__': main()
