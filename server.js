/* ============================================================
   Wayfare — Event Transportation Tracking relay
   In-memory Socket.io hub (no database), same approach as the FeTNA build.
   Carries: guests (pickups), drivers + vehicles, live driver locations,
   status changes, and a timestamped event timeline.
   ============================================================ */
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const fs = require("fs");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.get("/", (_req, res) => res.send("Wayfare relay OK"));

// ---- in-memory state -------------------------------------------------
let guests = {};   // id -> guest record
let drivers = {};  // id -> driver record (with vehicle + live location)
let events = [];    // [{ id, guestId, type, label, ts }]

const VENUE = { name: "Convention Center", lat: 40.5188, lng: -74.3294 };

// ---- seed demo fleet + a few pickups so the dashboard isn't empty -----
function seed() {
  drivers = {
    D1: { id:"D1", name:"Ravi Kumar",   phone:"+1 555 0101", code:"1111",
          vehicle:{ make:"Toyota", model:"Sienna", color:"Silver", plate:"EVT-1180", capacity:6, luggage:4 },
          status:"available", lat:40.512, lng:-74.342, ts:0 },
    D2: { id:"D2", name:"Maria Lopez",  phone:"+1 555 0102", code:"2222",
          vehicle:{ make:"Honda", model:"Odyssey", color:"Black", plate:"EVT-2204", capacity:6, luggage:4 },
          status:"available", lat:40.524, lng:-74.318, ts:0 },
    D3: { id:"D3", name:"Sam Patel",    phone:"+1 555 0103", code:"3333",
          vehicle:{ make:"Ford", model:"Transit", color:"White", plate:"EVT-3390", capacity:12, luggage:10 },
          status:"available", lat:40.508, lng:-74.350, ts:0 },
  };
  const mk = (id,fn,ln,addr,lat,lng,time,pax,bags,driverId) => ({
    id, firstName:fn, lastName:ln, mobile:"+1 555 1"+id.slice(1).padStart(3,"0"),
    email:(fn+"."+ln+"@email.com").toLowerCase(), pickupAddress:addr, pickupLat:lat, pickupLng:lng,
    pickupTime:time, destination:VENUE.name, passengers:pax, bags:bags, large:false,
    instructions:"", notes:"", status:driverId?"assigned":"scheduled", driverId:driverId||null, ts:Date.now()
  });
  guests = {
    G1: mk("G1","Aarav","Shah","Newark Liberty Airport — Terminal B",40.6895,-74.1745,"09:30",2,2,"D1"),
    G2: mk("G2","Priya","Nair","Sheraton Edison Hotel",40.5205,-74.3470,"09:45",1,1,null),
    G3: mk("G3","John","Mathew","Hilton Garden Inn",40.5360,-74.3450,"10:00",3,4,"D2"),
    G4: mk("G4","Lena","Wong","Metropark Train Station",40.5709,-74.3290,"10:15",1,0,null),
    G5: mk("G5","Omar","Haddad","Courtyard by Marriott",40.5420,-74.3380,"10:20",2,2,null),
  };
  events = [];
}
seed();

const DB_FILE = "./wayfare-data.json";
function save(){ try{ fs.writeFileSync(DB_FILE, JSON.stringify({ guests, drivers, events })); }catch(e){} }
try{ if(fs.existsSync(DB_FILE)){ const d=JSON.parse(fs.readFileSync(DB_FILE,"utf8"));
  if(d.guests && Object.keys(d.guests).length) guests=d.guests;
  if(d.drivers && Object.keys(d.drivers).length) drivers=d.drivers;
  if(Array.isArray(d.events)) events=d.events;
} }catch(e){}

function logEvent(guestId, type, label){
  const e = { id:"E"+Date.now()+Math.random().toString(36).slice(2,6), guestId, type, label, ts:Date.now() };
  events.push(e); if(events.length>5000) events=events.slice(-5000);
  io.emit("event", e); save();
}
function snapshot(){ return { guests, drivers, events: events.slice(-400), venue: VENUE }; }

io.on("connection", (socket) => {
  socket.emit("snapshot", snapshot());

  // ---------- ADMIN / DISPATCH ----------
  socket.on("getSnapshot", () => socket.emit("snapshot", snapshot()));

  socket.on("saveGuest", (g, ack) => {
    if (!g || !g.firstName) { if (typeof ack==="function") ack({ok:false}); return; }
    const id = g.id || ("G" + Date.now().toString(36));
    const prev = guests[id] || {};
    guests[id] = { ...prev, ...g, id, status: g.status || prev.status || "scheduled", ts: Date.now() };
    io.emit("guest", guests[id]);
    if (!prev.id) logEvent(id, "created", "Guest added");
    save(); if (typeof ack==="function") ack({ ok:true, id });
  });

  // Bulk import (array of guests) with email/mobile dedup → merge
  socket.on("importGuests", (rows, ack) => {
    let created=0, updated=0;
    (rows||[]).forEach(r => {
      if (!r || !r.firstName) return;
      const match = Object.values(guests).find(x =>
        (r.email && x.email && x.email.toLowerCase()===String(r.email).toLowerCase()) ||
        (r.mobile && x.mobile && x.mobile===r.mobile));
      if (match) { guests[match.id] = { ...match, ...r, id:match.id, ts:Date.now() }; updated++; io.emit("guest", guests[match.id]); }
      else { const id="G"+Date.now().toString(36)+Math.random().toString(36).slice(2,5);
             guests[id] = { status:"scheduled", driverId:null, destination:VENUE.name, ...r, id, ts:Date.now() };
             created++; io.emit("guest", guests[id]); }
    });
    save(); if (typeof ack==="function") ack({ ok:true, created, updated });
  });

  socket.on("assign", ({ guestId, driverId }) => {
    const g = guests[guestId]; if (!g) return;
    g.driverId = driverId || null;
    g.status = driverId ? (g.status==="scheduled"||!g.status ? "assigned" : g.status) : "scheduled";
    g.ts = Date.now();
    io.emit("guest", g);
    logEvent(guestId, "assigned", driverId ? ("Assigned to " + (drivers[driverId]?.name||driverId)) : "Unassigned");
  });

  socket.on("deleteGuest", ({ guestId }) => {
    if (guests[guestId]) { delete guests[guestId]; io.emit("guestGone", { id: guestId }); save(); }
  });

  // ---------- DRIVER ----------
  socket.on("driverHello", ({ id }) => { if (drivers[id]) socket.emit("youAre", drivers[id]); });

  socket.on("driverLoc", ({ id, lat, lng, heading }) => {
    const d = drivers[id]; if (!d) return;
    d.lat = lat; d.lng = lng; if (heading!=null) d.heading = heading; d.ts = Date.now();
    io.emit("driverLoc", { id, lat, lng, heading: d.heading, ts: d.ts });
  });

  socket.on("driverStatus", ({ id, status }) => {
    const d = drivers[id]; if (!d) return;
    d.status = status; d.ts = Date.now(); io.emit("driver", d);
  });

  // Driver advances a guest's trip status
  socket.on("guestStatus", ({ guestId, status }) => {
    const g = guests[guestId]; if (!g) return;
    g.status = status; g.ts = Date.now(); io.emit("guest", g);
    const labels = { en_route:"Driver en route", arrived:"Driver arrived", picked_up:"Guest picked up",
                     in_transit:"In transit", completed:"Drop-off complete", cancelled:"Cancelled" };
    logEvent(guestId, status, labels[status] || status);
    // reflect on driver status
    if (g.driverId && drivers[g.driverId]) {
      const map = { en_route:"on_route", arrived:"picking_up", picked_up:"in_transit", in_transit:"in_transit", completed:"available" };
      if (map[status]) { drivers[g.driverId].status = map[status]; io.emit("driver", drivers[g.driverId]); }
    }
  });

  socket.on("disconnect", () => {});
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log("Wayfare relay listening on :" + PORT));
