import { start, apply, state } from './engine.mjs';

const BROKERS = ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt'];
const utf8 = new TextEncoder();
const decode = new TextDecoder();
const ec = {name:'ECDSA', namedCurve:'P-256'};
const signatureAlgorithm = {name:'ECDSA', hash:'SHA-256'};
const b64 = bytes => btoa(String.fromCharCode(...bytes)).replaceAll('+','-').replaceAll('/','_').replace(/=+$/,'');
const un64 = str => Uint8Array.from(atob(str.replaceAll('-','+').replaceAll('_','/')), c=>c.charCodeAt(0));
const random = n => crypto.getRandomValues(new Uint8Array(n));
const wait = ms => new Promise(resolve=>setTimeout(resolve,ms));

export async function makeInvites() {
  const pair = await crypto.subtle.generateKey(ec,true,['sign','verify']);
  const key = await crypto.subtle.exportKey('jwk',pair.privateKey);
  const shared = [b64(random(16)), b64(random(32)), key.x, key.y].join('.');
  return {red:'#red='+shared+'.'+key.d, blue:'#blue='+shared};
}

export class Room {
  constructor(onState,onStatus) {
    this.onState=onState;
    this.onStatus=onStatus;
    this.clients=[];
    this.rev=-1;
    this.peerSeen=0;
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
      const record=saved?JSON.parse(saved):{game:start(),rev:0,receipt:null,instance:crypto.randomUUID()};
      this.persist(record);
      this.showHost();
    }
    this.status();
    for(const url of BROKERS)this.connectBroker(url);
    this.timer=setInterval(()=>{this.status();if(!this.host)this.hello();},8000);
    document.addEventListener('visibilitychange',()=>{if(!document.hidden&&!this.closed&&!this.host)this.hello();});
  }
  persist(record) {
    localStorage.setItem(this.saveKey,JSON.stringify(record));
    this.record=record;
    this.rev=record.rev;
  }
  showHost() {this.onState({revision:this.rev,data:state(this.record.game,0,this.id.slice(0,6).toUpperCase())});}
  status(){
    if(this.closed)return;
    const online=this.clients.some(c=>c.connected);
    const peer=Date.now()-this.peerSeen<26000;
    this.onStatus(!online?'正在连接公网中转…':peer?'● 红蓝双方已连接':this.host?'● 已连接公网 · 等待蓝方':'● 已连接公网 · 等待红方');
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
  hello(){this.transmit({kind:'hello',rev:this.rev,instance:this.instance||null}).catch(()=>{});}
  snapshot(extra={}) {
    return this.transmit({kind:'state',instance:this.record.instance,rev:this.rev,receipt:this.record.receipt,data:state(this.record.game,1,this.id.slice(0,6).toUpperCase()),...extra});
  }
  commit(seat,action,receipt) {
    const game=apply(structuredClone(this.record.game),seat,action);
    this.persist({...this.record,game,rev:this.rev+1,receipt});
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
    this.peerSeen=Date.now();this.status();
    if(this.host){
      if(msg.kind==='hello') {
        if(msg.rev!==this.rev||msg.instance!==this.record.instance)await this.snapshot();
        else await this.transmit({kind:'alive'});
        return;
      }
      if(msg.kind!=='move'||typeof msg.rid!=='string'||!/^[0-9a-f-]{36}$/.test(msg.rid))return;
      if(msg.rid===this.record.receipt){await this.snapshot();return;}
      if(msg.rev!==this.rev||msg.instance!==this.record.instance){await this.snapshot({refused:msg.rid,reason:'棋盘已更新，请重新选择。'});return;}
      try{this.commit(1,msg.action,msg.rid);}
      catch(e){await this.snapshot({refused:msg.rid,reason:e.message});return;}
      await this.snapshot();
    }else if(msg.kind==='ready'){this.hello();}
    else if(msg.kind==='state'){
      if(this.instance&&this.instance!==msg.instance){this.onState({reset:true});this.rev=-1;}
      this.instance=msg.instance;
      if(msg.rev>=this.rev){this.rev=msg.rev;this.onState({revision:msg.rev,data:msg.data});}
      if(this.pending&&this.pending.rid===msg.receipt)this.pending.done=true;
      if(this.pending&&msg.refused===this.pending.rid)this.pending.error=msg.reason;
    }
  }
  async move(action) {
    if(this.host){this.commit(0,action,this.record.receipt);await this.snapshot();return;}
    if(this.rev<0||Date.now()-this.peerSeen>=26000)throw Error('正在等待红方重新连接。');
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
