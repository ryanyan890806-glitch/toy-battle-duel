import { start, apply, state, MAPS } from './engine.mjs?v=chest-radio-2';

const BROKERS = ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt'];
const utf8 = new TextEncoder();
const decode = new TextDecoder();
const ec = {name:'ECDSA', namedCurve:'P-256'};
const signatureAlgorithm = {name:'ECDSA', hash:'SHA-256'};
const b64 = bytes => btoa(String.fromCharCode(...bytes)).replaceAll('+','-').replaceAll('/','_').replace(/=+$/,'');
const un64 = str => Uint8Array.from(atob(str.replaceAll('-','+').replaceAll('_','/')), c=>c.charCodeAt(0));
const random = n => crypto.getRandomValues(new Uint8Array(n));
const wait = ms => new Promise(resolve=>setTimeout(resolve,ms));
const heartbeatMs = 3000;
const peerTimeoutMs = 13000;
const newLobby = mapId => ({mapId, ticket:crypto.randomUUID(), ready:[false,false], sequence:[0,0]});
const radioStations = new Set(['chillsynth','nightride','spacesynth','darksynth','cnclassic','cnpop']);

export async function makeInvites() {
  const pair = await crypto.subtle.generateKey(ec,true,['sign','verify']);
  const key = await crypto.subtle.exportKey('jwk',pair.privateKey);
  const shared = [b64(random(16)), b64(random(32)), key.x, key.y].join('.');
  return {red:'#red='+shared+'.'+key.d, blue:'#blue='+shared};
}

export class Room {
  constructor(onState,onStatus,onRadio=()=>{}) {
    this.onState=onState;
    this.onStatus=onStatus;
    this.onRadio=onRadio;
    this.radioRevision=-1;
    this.clients=[];
    this.rev=-1;
    this.peerSeen=0;
    this.session=crypto.randomUUID();
    this.closed=false;
  }
  async open(fragment) {
    const seats=new URLSearchParams(fragment.slice(1));
    this.host=seats.has('red');
    const parts=(seats.get(this.host?'red':'blue')||'').split('.');
    if(parts.length!==(this.host?5:4) || !/^[\w-]{22}$/.test(parts[0]) || parts.slice(1).some(s=>!(/^[\w-]{43}$/.test(s)))) throw Error('邀请链接不完整，请重新复制整条链接。');
    const [id,secret,x,y,d]=parts;
    this.id=id;
    this.topic='toybattle/v1/'+id;
    this.invites=this.host?{red:fragment,blue:'#blue='+parts.slice(0,4).join('.')}:null;
    this.cipher=await crypto.subtle.importKey('raw',un64(secret),{name:'AES-GCM'},false,['encrypt','decrypt']);
    this.signer=await crypto.subtle.importKey('jwk',{kty:'EC',crv:'P-256',x,y,...(this.host?{d}:{})},ec,false,[this.host?'sign':'verify']);
    if(this.host) {
      if(navigator.locks) {
        await new Promise((resolve,reject)=>navigator.locks.request('toy-battle:'+id,{ifAvailable:true}, lock=>{
          if(!lock){reject(Error('红方已在另一个标签页打开，请回到那个页面。'));return;}
          resolve();return new Promise(done=>{this.unlock=done;});
        }).catch(reject));
      }
      this.saveKey='toy-battle/room/v1/'+id;
      const saved=localStorage.getItem(this.saveKey);
      const record=saved?JSON.parse(saved):{game:null,lobby:newLobby('castle'),rev:0,receipt:null,instance:crypto.randomUUID()};
      if (saved && !Object.hasOwn(record,'lobby')) {
        const played=record.game.count>0 || record.game.phase || Object.values(record.game.board).some(stack=>stack.length);
        record.lobby=played?null:newLobby(record.game.map.id);
        if (!played) record.game=null;
        record.rev++;
      } else if (saved && record.lobby) {
        record.lobby=newLobby(record.lobby.mapId);
        record.rev++;
      }
      if (!record.radio) record.radio={station:'chillsynth',revision:0,changedBy:null};
      this.persist(record);
      this.acceptRadio(record.radio);
      this.showHost();
    }
    this.status();
    for(const url of BROKERS)this.connectBroker(url);
    this.timer=setInterval(()=>{this.status();if(!this.host)this.hello();},heartbeatMs);
    document.addEventListener('visibilitychange',()=>{if(!document.hidden&&!this.closed&&!this.host)this.hello();});
  }
  persist(record) {
    localStorage.setItem(this.saveKey,JSON.stringify(record));
    this.record=record;
    this.rev=record.rev;
  }
  view(seat) {
    const room=this.id.slice(0,6).toUpperCase();
    if (this.record.lobby) return {stage:'lobby',room,you:seat,...this.record.lobby};
    return {...state(this.record.game,seat,room),stage:'playing'};
  }
  showHost() {this.onState({revision:this.rev,data:this.view(0)});}
  presence() {
    const online=this.clients.some(c=>c.connected);
    const peer=online && Date.now()-this.peerSeen<peerTimeoutMs;
    return this.host?[online,peer]:[peer,online];
  }
  status(){
    if(this.closed)return;
    const seats=this.presence(), me=this.host?0:1;
    if(this.host && this.record?.lobby) this.clearOfflineReady(seats);
    this.onStatus(!seats[me]?'正在连接公网中转…':seats[1-me]?'● 红蓝双方已连接':this.host?'● 已连接公网 · 等待蓝方':'● 已连接公网 · 等待红方',seats);
  }
  clearOfflineReady(seats) {
    const lobby=this.record.lobby;
    if (!lobby.ready.some((r,i)=>r&&!seats[i])) return;
    const ready=lobby.ready.map((r,i)=>r&&seats[i]);
    const sequence=lobby.sequence.map((n,i)=>n+Number(ready[i]!==lobby.ready[i]));
    this.persist({...this.record,lobby:{...lobby,ready,sequence},rev:this.rev+1});
    this.showHost();
    this.snapshot().catch(()=>{});
  }
  connectBroker(url) {
    const client=globalThis.mqtt.connect(url,{protocolVersion:4,clean:true,keepalive:30,connectTimeout:6500,reconnectPeriod:2700+Math.random()*1800,queueQoSZero:false,clientId:'tb_'+b64(random(12))});
    this.clients.push(client);
    const channel=this.topic+(this.host?'/red':'/blue');
    client.on('connect',()=>client.subscribe(channel,{qos:0},error=>{
      if(!error){this.status();if(!this.host)this.hello();else this.transmit({kind:'ready'}).catch(()=>{});}
    }));
    client.on('close',()=>this.status());
    client.on('error',()=>this.status());
    client.on('message',(topic,payload)=>{if(topic===channel&&!this.closed)this.receive(topic,payload).catch(()=>{});});
  }
  async transmit(message) {
    if(this.closed||!this.clients.some(c=>c.connected))return;
    const channel=this.topic+(this.host?'/blue':'/red');
    let body=utf8.encode(JSON.stringify(message));
    if(this.host){
      const signed=await crypto.subtle.sign(signatureAlgorithm,this.signer,body);
      const packed=new Uint8Array(signed.byteLength+body.length);packed.set(new Uint8Array(signed));packed.set(body,signed.byteLength);body=packed;
    }
    const iv=random(12);
    const enc=await crypto.subtle.encrypt({name:'AES-GCM',iv,additionalData:utf8.encode(channel)},this.cipher,body);
    const wire=new Uint8Array(12+enc.byteLength);wire.set(iv);wire.set(new Uint8Array(enc),12);
    for(const client of this.clients)if(client.connected)client.publish(channel,wire,{qos:0,retain:false});
  }
  hello(){this.transmit({kind:'hello',session:this.session,rev:this.rev,radioRevision:this.radioRevision,instance:this.instance||null}).catch(()=>{});}
  snapshot(extra={}) {
    return this.transmit({kind:'state',instance:this.record.instance,rev:this.rev,receipt:this.record.receipt,data:this.view(1),radio:this.record.radio,...extra});
  }
  acceptRadio(radio) {
    if (!radio || !radioStations.has(radio.station) || !Number.isSafeInteger(radio.revision) || radio.revision<=this.radioRevision) return;
    this.radioRevision=radio.revision;
    this.onRadio(radio);
  }
  broadcastRadio(extra={}) {
    return this.transmit({kind:'radio-state',instance:this.record.instance,radio:this.record.radio,...extra});
  }
  setRadio(seat,station,receipt) {
    if (!radioStations.has(station)) throw Error('请选择列表中的电台');
    const receipts=this.record.radioReceipts||[];
    if (receipt && receipts.includes(receipt)) return;
    const radio={station,revision:this.record.radio.revision+1,changedBy:seat};
    this.persist({...this.record,radio,radioReceipts:receipt?[...receipts,receipt].slice(-32):receipts});
    this.acceptRadio(radio);
  }
  async selectRadio(station) {
    if (!radioStations.has(station)) throw Error('请选择列表中的电台');
    if (this.host) {
      this.setRadio(0,station);
      await this.broadcastRadio();
      return;
    }
    if (this.rev<0 || !this.presence()[0]) throw Error('等待红方上线后即可切台');
    if (this.radioPending) throw Error('正在同步电台，请稍候');
    const pending={kind:'radio-select',rid:crypto.randomUUID(),instance:this.instance,station};
    this.radioPending=pending;
    try {
      for(let attempt=0;attempt<4;attempt++) {
        await this.transmit(pending);
        for(let tick=0;tick<12&&!pending.done&&!pending.error;tick++)await wait(150);
        if(pending.done)return;
        if(pending.error)throw Error(pending.error);
      }
      this.hello();
      throw Error('电台同步超时，请稍后重试');
    } finally {this.radioPending=null;}
  }
  commit(seat,action,receipt) {
    let {game,lobby}=this.record;
    const op=action.action;
    if (op==='select_map') {
      if (seat!==0 || !Object.hasOwn(MAPS,action.map)) throw Error('仅房主可选择可对战地图');
      if (lobby && action.map===lobby.mapId) return;
      lobby=newLobby(action.map);game=null;
    } else if (op==='restart' && !lobby) {
      lobby=newLobby(game.map.id);game=null;
    } else if (lobby) {
      if (op==='set_ready') {
        if (!this.presence()[seat] || action.ticket!==lobby.ticket || action.sequence!==lobby.sequence[seat] || typeof action.ready!=='boolean') throw Error('房间已更新，请重新确认准备状态');
        lobby=structuredClone(lobby);lobby.ready[seat]=action.ready;lobby.sequence[seat]++;
      } else if (op==='begin') {
        if (seat!==0 || action.ticket!==lobby.ticket || !lobby.ready.every(Boolean) || !this.presence().every(Boolean)) throw Error('双方在线并准备后，房主才能开始');
        game=start(lobby.mapId);lobby=null;
      } else throw Error('请先在房间准备并开始对局');
    } else {
      game=apply(structuredClone(game),seat,action);
    }
    this.persist({...this.record,game,lobby,rev:this.rev+1,receipt});
    this.showHost();
  }
  async receive(channel,wire) {
    if(wire.length<30||wire.length>96000 || (this.host&&wire.length>2400))return;
    const raw=new Uint8Array(await crypto.subtle.decrypt({name:'AES-GCM',iv:wire.slice(0,12),additionalData:utf8.encode(channel)},this.cipher,wire.slice(12)));
    let body=raw;
    if(!this.host){
      body=raw.slice(64);
      if(!await crypto.subtle.verify(signatureAlgorithm,this.signer,raw.slice(0,64),body))return;
    }
    const msg=JSON.parse(decode.decode(body));
    if(this.host && this.record.lobby) {
      this.clearOfflineReady(this.presence());
      if(msg.kind==='hello' && this.peerSession && msg.session!==this.peerSession) this.clearOfflineReady([true,false]);
    }
    if(this.host && msg.kind==='hello') this.peerSession=msg.session;
    this.peerSeen=Date.now();this.status();
    if(this.host){
      if(msg.kind==='hello') {
        if(msg.rev!==this.rev||msg.instance!==this.record.instance)await this.snapshot();
        else await this.transmit({kind:'alive'});
        if(msg.radioRevision!==this.radioRevision)await this.broadcastRadio();
        return;
      }
      if(msg.kind==='radio-select') {
        if(typeof msg.rid!=='string'||!/^[0-9a-f-]{36}$/.test(msg.rid))return;
        try {
          if(msg.instance!==this.record.instance)throw Error('房间已重新连接，请重试切台');
          this.setRadio(1,msg.station,msg.rid);
          await this.broadcastRadio({radioReceipt:msg.rid});
        } catch(e) {await this.broadcastRadio({radioRefused:msg.rid,reason:e.message});}
        return;
      }
      if(msg.kind!=='move'||typeof msg.rid!=='string'||!/^[0-9a-f-]{36}$/.test(msg.rid))return;
      if(msg.rid===this.record.receipt){await this.snapshot();return;}
      const readyIntent=msg.action?.action==='set_ready' && this.record.lobby && msg.action.ticket===this.record.lobby.ticket;
      if((msg.rev!==this.rev&&!readyIntent)||msg.instance!==this.record.instance){await this.snapshot({refused:msg.rid,reason:'房间已更新，请重新选择。'});return;}
      try{this.commit(1,msg.action,msg.rid);}
      catch(e){await this.snapshot({refused:msg.rid,reason:e.message});return;}
      await this.snapshot();
    }else if(msg.kind==='ready'){this.hello();}
    else if(msg.kind==='radio-state'){
      if(msg.instance!==this.instance){this.hello();return;}
      this.acceptRadio(msg.radio);
      if(this.radioPending?.rid===msg.radioReceipt)this.radioPending.done=true;
      if(this.radioPending?.rid===msg.radioRefused)this.radioPending.error=msg.reason;
    }
    else if(msg.kind==='state'){
      if(this.instance&&this.instance!==msg.instance){this.onState({reset:true});this.rev=-1;this.radioRevision=-1;}
      this.instance=msg.instance;
      this.acceptRadio(msg.radio);
      if(msg.rev>=this.rev){this.rev=msg.rev;this.onState({revision:msg.rev,data:msg.data});}
      if(this.pending&&this.pending.rid===msg.receipt)this.pending.done=true;
      if(this.pending&&msg.refused===this.pending.rid)this.pending.error=msg.reason;
    }
  }
  async move(action) {
    if(this.host){this.commit(0,action,this.record.receipt);await this.snapshot();return;}
    if(this.rev<0||!this.presence()[0])throw Error('正在等待红方重新连接。');
    const pending={rid:crypto.randomUUID(),kind:'move',instance:this.instance,rev:this.rev,action};
    this.pending=pending;
    try{
      for(let attempt=0;attempt<6;attempt++){
        await this.transmit(pending);
        for(let tick=0;tick<12&&!pending.done&&!pending.error;tick++)await wait(150);
        if(pending.done)return;
        if(pending.error)throw Error(pending.error);
      }
      this.hello();throw Error('尚未收到红方确认，请待同步后查看棋盘。');
    }finally{this.pending=null;}
  }
  close(){this.closed=true;clearInterval(this.timer);for(const client of this.clients)client.end(true);this.unlock?.();}
}
