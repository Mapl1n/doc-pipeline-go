package router

import "github.com/gin-gonic/gin"

func serveWebUI(c *gin.Context) {
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.String(200, uiHTML)
}

const uiHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>🔄 文档处理流水线</title>
<style>
:root{--bg:#0f172a;--card:#1e293b;--border:#334155;--text:#e2e8f0;--muted:#94a3b8;--accent:#f59e0b;--green:#22c55e;--red:#ef4444;--purple:#8b5cf6}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:var(--bg);color:var(--text)}
.header{background:var(--card);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;justify-content:space-between;align-items:center}
.header h1{font-size:18px}.container{max-width:800px;margin:20px auto;padding:0 20px}
.card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px;margin-bottom:16px}
.card h3{font-size:14px;margin-bottom:12px;color:var(--accent)}
.btn{padding:8px 18px;border-radius:6px;border:none;cursor:pointer;font-size:13px;font-weight:500}
.btn-primary{background:var(--accent);color:#000;width:100%}
input{width:100%;background:var(--bg);border:1px solid var(--border);color:var(--text);padding:8px 12px;border-radius:6px;font-size:13px}
.upload-zone{border:2px dashed var(--border);border-radius:10px;padding:30px;text-align:center;cursor:pointer;margin-bottom:12px;transition:all .2s}
.upload-zone:hover{border-color:var(--accent)}
.pipeline{display:flex;align-items:center;gap:8px;margin:20px 0;flex-wrap:wrap;justify-content:center}
.stage{display:flex;align-items:center;gap:8px}
.step{padding:8px 14px;border-radius:20px;font-size:12px;font-weight:500;background:var(--bg);border:1px solid var(--border);text-align:center;transition:all .5s}
.step.active{background:var(--accent);color:#000;border-color:var(--accent);transform:scale(1.1);box-shadow:0 0 16px rgba(245,158,11,0.4)}
.step.done{background:var(--green);color:#000;border-color:var(--green)}
.step.failed{background:var(--red);color:#fff}
.arrow{font-size:18px;color:var(--muted)}
.progress-bar{height:6px;background:var(--bg);border-radius:3px;overflow:hidden;margin:12px 0}
.progress-fill{height:100%;background:var(--accent);border-radius:3px;transition:width .3s}
.task-row{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px}
.badge-pending{background:#ca8a04;color:#000}.badge-processing{background:var(--purple);color:#fff}
.badge-done{background:var(--green);color:#000}.badge-failed{background:var(--red);color:#fff}
#toast{position:fixed;top:20px;right:20px;z-index:9999}
.toast-msg{padding:10px 18px;border-radius:8px;font-size:12px;margin-bottom:8px}
.toast-success{background:#065f46;color:#6ee7b7}.toast-error{background:#7f1d1d;color:#fca5a5}
</style></head>
<body>
<div class="header"><h1>🔄 分布式文档处理流水线</h1><span id="pending" style="font-size:13px;color:var(--muted)"></span></div>
<div id="toast"></div>
<div class="container">
  <div class="card"><h3>📤 提交文档</h3>
    <div class="upload-zone" id="dropZone" onclick="document.getElementById('fileInput').click()">
      <p style="font-size:28px">📄</p><p>上传 PDF/DOCX 开始处理</p>
      <p style="font-size:11px;color:var(--muted);margin-top:4px">Upload → Parse → Classify → Index</p>
    </div>
    <input type="file" id="fileInput" accept=".pdf,.docx,.txt" style="display:none" onchange="uploadFile()">
    <div id="status" style="font-size:12px;color:var(--muted)"></div>
  </div>
  <div id="monitor" class="card"><h3>📊 处理监控</h3><p style="color:var(--muted);font-size:13px">提交文件以查看处理进度</p></div>
</div>
<script>
const API='/api';let ws=null;
document.getElementById('dropZone').ondragover=e=>{e.preventDefault()};
document.getElementById('dropZone').ondrop=e=>{e.preventDefault();const f=e.dataTransfer.files[0];if(f)doUpload(f)};
function uploadFile(){const f=document.getElementById('fileInput').files[0];if(f)doUpload(f)}
async function doUpload(file){
  document.getElementById('status').innerHTML='<span style="color:var(--accent)">⏳ 上传中...</span>';
  const fd=new FormData();fd.append('file',file);
  try{
    const r=await fetch(API+'/upload',{method:'POST',body:fd});const d=await r.json();
    if(d.code===0){
      toast('✅ 任务已提交: '+d.data.task_id.substring(0,8)+'...','success');
      startMonitoring(d.data.task_id,file.name);
    }else{toast(d.message,'error')}
  }catch(e){toast(e.message,'error')}
  document.getElementById('status').innerHTML='';
}
function startMonitoring(taskID,filename){
  if(ws)ws.close();
  const stages=['upload','parse','classify','index','complete'];
  document.getElementById('monitor').innerHTML='<h3>📊 处理中: '+filename+'</h3><div class="pipeline">'+
    stages.map((s,i)=>'<div class="stage">'+(i>0?'<div class="arrow">→</div>':'')+'<div class="step" id="step-'+s+'">'+({upload:'📤 上传',parse:'🔍 解析',classify:'🏷️ 分类',index:'📇 索引',complete:'✅ 完成'})[s]+'</div></div>').join('')+
    '</div><div class="progress-bar"><div class="progress-fill" id="pbar" style="width:0%"></div></div><p id="stageInfo" style="font-size:12px;color:var(--muted);text-align:center">等待 Worker...</p><div id="taskResult"></div>';
  const proto=location.protocol==='https:'?'wss':'ws';
  ws=new WebSocket(proto+'://'+location.host+API+'/ws/progress?task_id='+taskID);
  ws.onmessage=function(e){
    const event=JSON.parse(e.data);
    document.getElementById('pbar').style.width=(event.progress*100)+'%';
    document.getElementById('stageInfo').textContent=event.stage+' · '+(event.progress*100).toFixed(0)+'%';
    document.querySelectorAll('.step').forEach(s=>s.classList.remove('active','done','failed'));
    stages.forEach(s=>{
      const el=document.getElementById('step-'+s);
      if(!el)return;
      const si=stages.indexOf(s),ci=stages.indexOf(event.stage);
      if(si<ci)el.classList.add('done');
      else if(si===ci){
        if(event.status==='failed')el.classList.add('failed');
        else el.classList.add('active');
      }
    });
    if(event.status==='done'){
      document.getElementById('step-complete').classList.add('done');
      document.getElementById('stageInfo').textContent='✅ 处理完成!';
      toast('任务完成','success');
    }
    if(event.status==='failed'){
      document.getElementById('taskResult').innerHTML='<p style="color:var(--red);margin-top:12px">❌ '+event.error+'</p>';
      toast('处理失败: '+event.error,'error');
    }
  };
}
function toast(msg,type){const e=document.getElementById('toast'),d=document.createElement('div');d.className='toast-msg toast-'+type;d.textContent=msg;e.appendChild(d);setTimeout(()=>d.remove(),3000)}
</script></body></html>`
