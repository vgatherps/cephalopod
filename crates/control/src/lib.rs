//! Unix-socket control plane for assigning stream ids and tracking player sessions.
//!
//! Players send no data after the connection handshake.

use ceph_proto::{Control, Retire, pack};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

pub struct Listener {
    socket: UnixListener,
    prefix: Vec<u8>,
    free: Vec<u32>,
    live: Vec<Option<(u32, UnixStream)>>,
    next_uid: u32,
    control: Sender<Control>,
    retire: Receiver<Retire>,
}

impl Listener {
    pub fn bind(
        path: &Path,
        prefix: &str,
        max_streams: u32,
        control: Sender<Control>,
        retire: Receiver<Retire>,
    ) -> io::Result<Listener> {
        let prefix = prefix.as_bytes().to_vec();
        if u16::try_from(prefix.len()).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shared-memory prefix exceeds u16 wire length",
            ));
        }
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let socket = UnixListener::bind(path)?;
        socket.set_nonblocking(true)?;
        let free = (0..max_streams).rev().collect();
        let live = (0..max_streams).map(|_| None).collect();
        Ok(Listener {
            socket,
            prefix,
            free,
            live,
            next_uid: 1,
            control,
            retire,
        })
    }

    pub fn run(mut self) -> io::Result<()> {
        loop {
            let mut slots = Vec::with_capacity(self.live.len());
            let mut pollfds = Vec::with_capacity(self.live.len() + 1);
            pollfds.push(libc::pollfd {
                fd: self.socket.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            });
            for (slot, entry) in self.live.iter().enumerate() {
                if let Some((_, socket)) = entry {
                    slots.push(slot as u32);
                    pollfds.push(libc::pollfd {
                        fd: socket.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    });
                }
            }

            let result = unsafe {
                libc::poll(pollfds.as_mut_ptr(), pollfds.len() as libc::nfds_t, 10)
            };
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }

            if pollfds[0].revents & libc::POLLIN != 0 && !self.accept_all()? {
                return Ok(());
            }

            for (index, slot) in slots.into_iter().enumerate() {
                let revents = pollfds[index + 1].revents;
                if revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                    // Players send no data after connecting. A readable event means the player is gone.
                    let (uid, socket) = self.live[slot as usize]
                        .take()
                        .expect("polled slot must still be live");
                    drop(socket);
                    self.free.push(slot);
                    if self.control.send(Control::Close { slot, uid }).is_err() {
                        return Ok(());
                    }
                }
            }

            loop {
                match self.retire.try_recv() {
                    Ok(retire) => {
                        let (slot, uid) = match retire {
                            Retire::Kicked { slot, uid } | Retire::Finished { slot, uid } => {
                                (slot, uid)
                            }
                        };
                        let entry = &mut self.live[slot as usize];
                        if entry.as_ref().is_some_and(|(live_uid, _)| *live_uid == uid) {
                            *entry = None;
                            self.free.push(slot);
                            if self
                                .control
                                .send(Control::Close { slot, uid })
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Ok(()),
                }
            }
        }
    }

    fn accept_all(&mut self) -> io::Result<bool> {
        for _ in 0..64 {
            let (mut socket, _) = match self.socket.accept() {
                Ok(pair) => pair,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(true),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(slot) = self.free.pop() else {
                continue;
            };
            let uid = self.next_uid;
            self.next_uid = self.next_uid.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "stream uid space exhausted")
            })?;
            // Send `Open` before the reply so the player's first record cannot precede it.
            // A failed reply sends the matching `Close`.
            if self.control.send(Control::Open { slot, uid }).is_err() {
                return Ok(false);
            }
            let reply = socket
                .set_nonblocking(false)
                .and_then(|()| {
                    socket.set_write_timeout(Some(std::time::Duration::from_millis(100)))
                })
                .and_then(|()| socket.write_all(&pack(slot, uid).to_le_bytes()))
                .and_then(|()| socket.write_all(&(self.prefix.len() as u16).to_le_bytes()))
                .and_then(|()| socket.write_all(&self.prefix));
            if reply.is_err() {
                drop(socket);
                let sent = self.control.send(Control::Close { slot, uid }).is_ok();
                self.free.push(slot);
                if !sent {
                    return Ok(false);
                }
                continue;
            }
            self.live[slot as usize] = Some((uid, socket));
        }
        Ok(true)
    }
}

pub struct Session {
    pub stream: u64,
    pub prefix: String,
    socket: UnixStream,
}

pub fn connect(path: &Path) -> io::Result<Session> {
    let mut socket = UnixStream::connect(path)?;
    let mut stream = [0_u8; 8];
    let mut length = [0_u8; 2];
    socket.read_exact(&mut stream)?;
    socket.read_exact(&mut length)?;
    let mut prefix = vec![0_u8; u16::from_le_bytes(length) as usize];
    socket.read_exact(&mut prefix)?;
    let prefix = String::from_utf8(prefix)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(Session {
        stream: u64::from_le_bytes(stream),
        prefix,
        socket,
    })
}

impl Session {
    pub fn closed(&self) -> bool {
        let mut pollfd = libc::pollfd {
            fd: self.socket.as_raw_fd(),
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut pollfd, 1, 0) };
        result > 0 && pollfd.revents & (libc::POLLIN | libc::POLLHUP) != 0
    }
}
