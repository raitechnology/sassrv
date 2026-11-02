# AGENTS.md — sassrv

Rai SASS RV: the TIBCO Rendezvous / SASS wire protocols as C++ libraries
and tools. `libsassrv` is the RV daemon-side protocol (`EvRvService`,
`EvRvClient`, the subscription manager `RvSubscriptionDB`, fault
tolerance `ft.h`); `librv5lib`/`librv7lib`/`librv7ftlib` are drop-in
C/C++ client APIs (rv5, rv7/tibrv, rv7 FT) built on it. Namespace
`rai::sassrv` (plus `rv7` for the tibrv-compatible API). Depends on
raikv, raimd, libdecnumber.

`CLAUDE.md` is a one-line `@AGENTS.md` import so Claude Code reads this
same file; put everything here.

## Workspace layout (injinj fleet convention)

- This repo is one of the `injinj` siblings. Every dependency is a sibling
  checkout (`../raikv`, `../raimd`, `../libdecnumber`); headers come from
  `-I../<dep>/include` and libraries from
  `../<dep>/FC43_x86_64/lib64/lib<dep>.a`. There is no install step between
  siblings — build the dependency, then build this repo.
- Dependencies are declared once, in `build_depends.mak`
  (`<dep>_dep := MAJOR.MINOR`, the rpm BuildRequires minimum). `./deps.sh`
  clones missing siblings from it, `./deps.sh -b` also builds them in
  dependency order, `./deps.sh -s` prints the commit of this repo and every
  sibling (the only record of what a release was built from).
- Whole-fleet build order lives in `~/injinj/GNUmakefile` (`make build`):
  openpgm → raikv → libdecnumber → raimd → linecook → lzf → rdbparser → h3 →
  HdrHistogram_c → sassrv → raids → natsmd → omm → raims → rvcache →
  raimdapi → ossl.

## Build

- `make` → `FC43_x86_64/bin`, `FC43_x86_64/lib64`. The build dir is
  `$(short_dist)$(lsb_dist_ver)_$(uname_m)$(port_extra)`: Fedora 43 →
  `FC43_x86_64`, Rocky 9.5 → `ROCKY9.5_x86_64`.
- `make port_extra=-g` → debug build in `FC43_x86_64-g`.
  `make port_extra=-mingw` → Windows cross build in `FC43_x86_64-mingw`
  (mingw64 toolchain; links the siblings' *master* checkouts).
- `NO_STL=1` builds with `-fno-rtti -fno-exceptions` and links with the C
  compiler; library code is written to survive that (error returns, no
  exceptions, no RTTI). Keep it that way.
- `make -j8` is safe; the makefile dependencies are complete. One trap: the
  **first `-j` run right after a compile error** can fail test executables
  with bogus undefined references (symbols from this repo's own lib)
  because they link against a library left half-updated by the failed run.
  Run plain `make` once, then `-j` works again. It is not a missing
  dependency.
- Warnings are clean. A new warning is a defect.
- If a sibling's public headers changed, rebuild the sibling first. A link
  warning like `lib<dep>.so.1.N, needed by ../<x>/.../lib<x>.so, not found`
  means sibling `<x>` was built against an older `<dep>` — rebuild `<x>`;
  at run time that warning is a hard failure.
- Ad hoc test program: capture the makefile's own lines once —
  `touch src/<any>.cpp; make -n | grep -- '-c src/<any>.cpp'` and
  `make -n | grep -- '-o FC43_x86_64/bin/' | head -1` — then reuse the
  compile line (minus `-c/-o`) and the link line's `-Wl,-rpath` list +
  `FC43_x86_64/lib64/libsassrv.a` + the lib tail. Run with
  `LD_LIBRARY_PATH` covering each sibling's `lib64`.
- `rv_server` is an rvd-compatible daemon, so every test and every
  dependent repo's test (rvcache, omm, raims) can run without a TIB
  install.
- Subject lists for replay/consumer tests: `top100.txt`, `top1000.txt`,
  `top10000.txt` (`RSF.REC.*` subjects matching the raimd captures).

## Versioning and release

- Version is `major_num.minor_num.patch_num-build_num` in `.copr/Makefile`
  (included by the GNUmakefile). `make spec_version` prints the
  `sassrv_dep := MAJOR.MINOR` line dependents pin in `build_depends.mak`.
- `rpm/sassrv.spec` carries placeholders (`Version: 999.999`, release
  `99999`, `BuildRequires: <dep> _<dep>_dep`); `make srpm` substitutes them
  from `build_depends.mak`. Keep the `_ensure_git` line in
  `.copr/Makefile`: COPR's `make_srpm` chroot has no git and every
  `$(shell git …)` would silently expand empty. A missing `<dep>_dep`
  entry fails the COPR build at install time, not link time.
- COPR: `injinj/dev` builds the `dev` branch of github.com/injinj/sassrv
  on push (webhook); `injinj/rel` builds github.com/raitechnology/sassrv.
  Remotes: `origin` = raitechnology (release), `ii` = injinj (dev).
  Chroots: epel-7/8/9/10, fedora-43/44/45/rawhide, x86_64.
- ABI convention: a dependent's `Requires` is `MAJOR.MINOR`, its
  `BuildRequires` the full version, and MINOR is bumped with any change to
  the C++ ABI of a public header, so a rebuilt dependent never silently
  loads an old `.so`. Bump `minor_num` here when public headers change,
  then re-pin and rebuild dependents.
- Link only against installable sibling libraries (`$(lnk_lib)`), never a
  sibling's object files; if a binary needs a sibling helper, move it into
  the sibling's library and release the sibling first.
- Signed rpms are published to https://www.raitechnology.com/repo/
  (COPR rel → download → sign → createrepo → rsync; tooling in `~/injinj/rel`).

## Code conventions

- C++11, two-space indent, `/* … */` block comments, aligned member
  comments; snake_case functions/variables, CamelCase types. System headers
  first, then project headers.
- Errors are return codes / status enums; no exceptions, no RTTI in library
  code. Raw pointers and placement-new over the repo's allocators are the
  norm; copy how neighbouring code allocates.
- Keep the generated `CMakeLists.txt` out of your edits — it is derived
  from the GNUmakefile (and slated for removal).

## Layout

- `include/sassrv/ev_rv.h` — `EvRvService`/`EvRvListen`: the daemon side
  (what `rv_server` and raims' rv transport instantiate).
- `ev_rv_client.h` — `EvRvClient`: connect to a daemon as a client
  (rvcache opens one per declared net).
- `submgr.h` — `RvSubscriptionDB`: interest tracking over `_INBOX`/listen
  start-stop advisories; also owns the sass3 channel used downstream.
- `rv5api.h`, `rv5cpp.h`, `rv7api.h`, `rv7cpp.h`, `rv7ftcpp.h` — client
  API surfaces. `ft.h` fault tolerance, `mc.h` multicast, `rv_host.h`
  host/daemon status.
- `src/` — 10 files; `test/` — one program per API (`rv5_api_test`,
  `rv5_cpp_test`, `rv7_test`, `*rv7test`, `intra_test`, `api_client`).
- Tools in `FC43_x86_64/bin`: `rv_server`, `rv_client` (`-x -s <port>
  -rate -f subjects.txt`), `rv_pub`, `rv_subtop`, `rv_ftmon`, `replayrv`
  (`-s <port> -f ../raimd/rsf4_rv.dat -m <count> -l <loop>`).
- `golang/` — Go bindings.

## Contracts and gotchas

- **Host subscription digest (`sc`/`sx`, 2026-11-01).** sassrv's `RvHost`
  adds `sc` and `sx` to HOST.START / HOST.STATUS (`ADV_SUBDIGEST`);
  `get_sub_digest()` computes them at send time from the host query reply
  view (all sockets of each session, subjects + patterns, `_`-prefixed
  subjects left out): `sc` = (session, subject) pairs, `sx` = xor of
  `kv_crc_c( sub, kv_crc_c( session ) )` -- session-seeded so two sessions
  on one subject don't cancel. `RvSubscriptionDB` keeps the same per
  `RvHostEntry` (`host_digest()` on add / deref / unsub_session) and on
  HOST.STATUS sets `RV_HOST_QUERY` on a mismatch. Skipped when the fields
  are absent (real rvd) or when filtered (`!is_all_subscribed`: unmatched
  LISTENs never arrive). Debug log: `> host <id> digest sc/sx … ok|skipped`
  / `! query host <id> digest`. Not yet exercised: a real mismatch.

- `make_rv_msg` switches on the full 32-bit codec id from
  `EvPublish.msg_enc`; a bare `ftype` byte falls to the default and the
  payload is wrapped as opaque `_data_`. Convert with raimd's
  `first_match/next_match` before publishing from a hash-entry type byte.
- Interest is advisory-driven (`_RV.INFO.SYSTEM.LISTEN.START/STOP`), so a
  consumer that never produces advisories (e.g. an rv7 client bound to a
  different daemon) is invisible to `RvSubscriptionDB`. Check which side
  of the daemon a subscriber sits on before concluding tracking is broken.
- Periodic stats in the tools print every 10 s; a test shorter than that
  never emits them.
- rv7 transport reconnect (`rv7_api.cpp`, since 2026-10-23) is a timer on
  the epoll thread, not a thread: `api_Transport::on_shutdown` →
  `arm_reconnect()` → `api_ReconnectTimer::timer_cb` every
  `RECONNECT_SECS` (1 s) calls `client.rv_connect()` directly when
  `client.fd == -1`, and after `RECONNECT_MAX_TICKS` (10) in the
  VERS..CONN handshake pushes `EV_CLOSE` so `release()` → `on_shutdown`
  retries. `on_connect` with `timer_armed` set = a reconnect: it removes
  the timer, refreshes `x.session` and calls `client.subscribe()` for every
  listener in `ht` (no `OP_SUBSCRIBE` pipe hop — it is already the epoll
  thread). The client is therefore only ever touched on the epoll thread;
  the api thread's only reconnect interaction is `EvPipe::close_tport`
  (from `DestroyTransport`, which sets `is_destroyed` *before* the close
  op) stopping the timer. Same shape as omm's `api_Transport::timer_cb`
  (omm lacks the handshake-stall tick, a latent gap there).
- Transport state advisories: `push_state()` synthesizes
  `_RV.ERROR.SYSTEM.RVD.DISCONNECTED` (only if `rv_state` was
  `DATA_RECV`, so failed retries don't spam) and
  `_RV.INFO.SYSTEM.RVD.CONNECTED` (reconnect only; the first connect is
  awaited by `CreateTransport`) with ADV_CLASS/SOURCE/NAME/DESC, delivers
  them to the `tibrvTransport_SetStateListener()` listener
  (`api_Listener::is_state`, held in `state_listener`, never in `ht` or
  subscribed; `DestroyEvent` must take the `is_state` branch) and then
  routes them through `on_rv_msg()` so ordinary tibrv listeners on
  `_RV.ERROR.SYSTEM.>` see them like a daemon advisory. Test rig:
  `/tmp/rctest.c` + `/tmp/rc-scenario.sh` (kill rv_server, `nc -l`
  stall, restart; expects DISCONNECTED, CONNECTED, then data on the
  re-subscribed subject).
- The replay rig (end to end with captured data):
  ```
  ms_server -d con.yaml -t rv.listen            # or: rv_server
  replayrv -s 7500 -f ../raimd/rsf4_rv.dat -m 10000 -l 0
  rv_client -x -s 7600 -rate -f top100.txt
  ```
  <!-- TODO: rv_server flags and default port; rv_pub usage -->

## Tests

- Build, then run the `*_test` / `*rv7test` binaries against a local
  `rv_server`. `sub.sh` is a subscriber helper. <!-- TODO: expected output -->

## Open items / history

<!-- TODO: fill from git log -->
