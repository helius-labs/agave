use {
    crate::{
        device::{
            DeviceQueue, RingConsumer, RingMmap, RingProducer, RxFillRing, TxCompletionRing,
            XdpDesc, mmap_ring,
        },
        umem::{Frame, FrameOffset, ReceivedFrame, RxFrameOffset, Umem},
    },
    libc::{
        AF_XDP, SOCK_RAW, SOL_XDP, XDP_COPY, XDP_MMAP_OFFSETS, XDP_PGOFF_RX_RING,
        XDP_PGOFF_TX_RING, XDP_RING_NEED_WAKEUP, XDP_RX_RING, XDP_TX_RING,
        XDP_UMEM_COMPLETION_RING, XDP_UMEM_FILL_RING, XDP_UMEM_PGOFF_COMPLETION_RING,
        XDP_UMEM_PGOFF_FILL_RING, XDP_USE_NEED_WAKEUP, XDP_ZEROCOPY, bind, getsockopt, sa_family_t,
        sendto, setsockopt, sockaddr, sockaddr_xdp, socket, socklen_t, xdp_mmap_offsets,
        xdp_umem_reg,
    },
    std::{
        io,
        marker::PhantomData,
        mem,
        os::fd::{AsFd, AsRawFd as _, BorrowedFd, FromRawFd as _, OwnedFd, RawFd},
        ptr,
        sync::atomic::Ordering,
    },
};

pub struct Socket<U: Umem> {
    fd: OwnedFd,
    dev_queue: DeviceQueue,
    umem: U,
}

impl<U: Umem> Socket<U> {
    #[allow(clippy::type_complexity)]
    pub fn new(
        dev_queue: DeviceQueue,
        umem: U,
        zero_copy: bool,
        rx_fill_ring_size: usize,
        rx_ring_size: usize,
        tx_completion_ring_size: usize,
        tx_ring_size: usize,
    ) -> Result<(Self, Rx<U::Frame>, Tx<U::Frame>), io::Error> {
        unsafe {
            let fd = socket(AF_XDP, SOCK_RAW, 0);
            if fd < 0 {
                return Err(Error::syscall(
                    "socket(AF_XDP, SOCK_RAW) failed",
                    io::Error::last_os_error(),
                )
                .into());
            }
            let fd = OwnedFd::from_raw_fd(fd);

            let reg = xdp_umem_reg {
                addr: umem.as_ptr() as u64,
                len: umem.len() as u64,
                chunk_size: umem.frame_size() as u32,
                headroom: 0,
                flags: 0,
                tx_metadata_len: 0,
            };

            if setsockopt(
                fd.as_raw_fd(),
                libc::SOL_XDP,
                libc::XDP_UMEM_REG,
                &reg as *const _ as *const libc::c_void,
                mem::size_of::<xdp_umem_reg>() as libc::socklen_t,
            ) < 0
            {
                return Err(Error::syscall(
                    "setsockopt(XDP_UMEM_REG) failed",
                    io::Error::last_os_error(),
                )
                .into());
            }

            for (ring, size) in [
                (XDP_UMEM_COMPLETION_RING, tx_completion_ring_size),
                (XDP_UMEM_FILL_RING, rx_fill_ring_size),
                (XDP_TX_RING, tx_ring_size),
                (XDP_RX_RING, rx_ring_size),
            ] {
                if ring == XDP_RX_RING && size == 0 {
                    // tx only
                    continue;
                }

                if setsockopt(
                    fd.as_raw_fd(),
                    SOL_XDP,
                    ring,
                    &size as *const _ as *const libc::c_void,
                    mem::size_of::<u32>() as socklen_t,
                ) < 0
                {
                    return Err(Error::syscall(
                        format!("setsockopt(SOL_XDP, ring={ring}, size={size}) failed",),
                        io::Error::last_os_error(),
                    )
                    .into());
                }
            }

            let mut offsets: xdp_mmap_offsets = mem::zeroed();
            let mut optlen = mem::size_of::<xdp_mmap_offsets>() as socklen_t;
            if getsockopt(
                fd.as_raw_fd(),
                SOL_XDP,
                XDP_MMAP_OFFSETS,
                &mut offsets as *mut _ as *mut libc::c_void,
                &mut optlen,
            ) < 0
            {
                return Err(Error::syscall(
                    "getsockopt(XDP_MMAP_OFFSETS) failed",
                    io::Error::last_os_error(),
                )
                .into());
            }

            let tx_completion_ring = TxCompletionRing::new(
                mmap_ring(
                    fd.as_raw_fd(),
                    tx_completion_ring_size.saturating_mul(mem::size_of::<u64>()),
                    &offsets.cr,
                    XDP_UMEM_PGOFF_COMPLETION_RING,
                )
                .map_err(|source| Error::syscall("mmap completion ring failed", source))?,
                tx_completion_ring_size as u32,
            );

            let mut rx_fill_ring = RxFillRing::new(
                mmap_ring(
                    fd.as_raw_fd(),
                    rx_fill_ring_size.saturating_mul(mem::size_of::<u64>()),
                    &offsets.fr,
                    XDP_UMEM_PGOFF_FILL_RING,
                )
                .map_err(|source| Error::syscall("mmap fill ring failed", source))?,
                rx_fill_ring_size as u32,
                fd.as_raw_fd(),
            );

            if zero_copy {
                // most drivers (intel) are buggy if ZC is enabled and the fill ring is not
                // pre-populated before calling bind()
                for _ in 0..rx_fill_ring_size {
                    let Some(frame) = umem.reserve() else {
                        return Err(Error::InsufficientUmemFrames {
                            required: rx_fill_ring_size,
                            available: umem.available(),
                        }
                        .into());
                    };
                    rx_fill_ring.write(frame).map_err(|RingFull(frame)| {
                        umem.release(frame);
                        Error::syscall(
                            "RX fill ring write failed",
                            io::ErrorKind::StorageFull.into(),
                        )
                    })?;
                }
                rx_fill_ring.commit();
            }

            let tx_ring = Some(TxRing::new(
                mmap_ring(
                    fd.as_raw_fd(),
                    tx_ring_size.saturating_mul(mem::size_of::<XdpDesc>()),
                    &offsets.tx,
                    XDP_PGOFF_TX_RING as u64,
                )
                .map_err(|source| Error::syscall("mmap tx ring failed", source))?,
                tx_ring_size as u32,
                fd.as_raw_fd(),
            ));

            let rx_ring = if rx_ring_size > 0 {
                Some(RxRing::new(
                    mmap_ring(
                        fd.as_raw_fd(),
                        rx_ring_size.saturating_mul(mem::size_of::<XdpDesc>()),
                        &offsets.rx,
                        XDP_PGOFF_RX_RING as u64,
                    )
                    .map_err(|source| Error::syscall("mmap rx ring failed", source))?,
                    rx_ring_size as u32,
                    fd.as_raw_fd(),
                ))
            } else {
                None
            };

            let sxdp = sockaddr_xdp {
                sxdp_family: AF_XDP as sa_family_t,
                // do NEED_WAKEUP and don't do zero copy for now for maximum compatibility
                sxdp_flags: XDP_USE_NEED_WAKEUP | if zero_copy { XDP_ZEROCOPY } else { XDP_COPY },
                sxdp_ifindex: dev_queue.if_index(),
                sxdp_queue_id: dev_queue.id().0 as u32,
                sxdp_shared_umem_fd: 0,
            };

            if bind(
                fd.as_raw_fd(),
                &sxdp as *const _ as *const sockaddr,
                mem::size_of::<sockaddr_xdp>() as socklen_t,
            ) < 0
            {
                return Err(Error::syscall(
                    format!(
                        "bind(AF_XDP, ifindex={}, queue={}, flags=0x{:x}) failed",
                        sxdp.sxdp_ifindex, sxdp.sxdp_queue_id, sxdp.sxdp_flags
                    ),
                    io::Error::last_os_error(),
                )
                .into());
            }

            let tx = Tx {
                completion: tx_completion_ring,
                ring: tx_ring,
            };
            let rx = Rx {
                fill: rx_fill_ring,
                ring: rx_ring,
            };
            Ok((
                Self {
                    fd,
                    dev_queue,
                    umem,
                },
                rx,
                tx,
            ))
        }
    }

    pub fn tx(
        queue: DeviceQueue,
        umem: U,
        zero_copy: bool,
        completion_size: usize,
        ring_size: usize,
    ) -> Result<(Self, Tx<U::Frame>), io::Error> {
        let (fill_size, rx_size) = if zero_copy {
            // See Socket::new() as to why this is needed
            let rx = queue
                .ring_sizes()
                .ok_or_else(|| io::Error::other("zero copy requires a set ring size"))?
                .rx;
            (rx, rx)
        } else {
            // no RX fill ring needed for TX only sockets
            (1, 0)
        };
        let (socket, _, tx) = Self::new(
            queue,
            umem,
            zero_copy,
            fill_size,
            rx_size,
            completion_size,
            ring_size,
        )?;
        Ok((socket, tx))
    }

    pub fn rx(
        queue: DeviceQueue,
        umem: U,
        zero_copy: bool,
        fill_size: usize,
        ring_size: usize,
    ) -> Result<(Self, Rx<U::Frame>), io::Error> {
        let (socket, rx, _) = Self::new(queue, umem, zero_copy, fill_size, ring_size, 0, 0)?;
        Ok((socket, rx))
    }

    pub fn queue(&self) -> &DeviceQueue {
        &self.dev_queue
    }

    pub fn umem(&self) -> &U {
        &self.umem
    }
}

impl<U: Umem> AsFd for Socket<U> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

pub struct Tx<F: Frame> {
    pub completion: TxCompletionRing,
    pub ring: Option<TxRing<F>>,
}

pub struct Rx<F: Frame> {
    pub fill: RxFillRing<F>,
    pub ring: Option<RxRing>,
}

pub struct TxRing<F: Frame> {
    mmap: RingMmap<XdpDesc>,
    producer: RingProducer,
    size: u32,
    fd: RawFd,
    _frame: PhantomData<F>,
}

#[derive(Debug)]
pub struct RingFull<F: Frame>(pub F);

impl<F: Frame> TxRing<F> {
    fn new(mmap: RingMmap<XdpDesc>, size: u32, fd: RawFd) -> Self {
        debug_assert!(size.is_power_of_two());
        Self {
            producer: RingProducer::new(mmap.producer, mmap.consumer, size),
            mmap,
            size,
            fd,
            _frame: PhantomData,
        }
    }

    pub fn write(&mut self, frame: F, options: u32) -> Result<(), RingFull<F>> {
        let Some(index) = self.producer.produce() else {
            return Err(RingFull(frame));
        };
        let index = index & self.size.saturating_sub(1);
        unsafe {
            let desc = self.mmap.desc.add(index as usize);
            desc.write(XdpDesc {
                addr: frame.offset().0 as u64,
                len: frame.len() as u32,
                options,
            });
        }
        Ok(())
    }

    pub fn needs_wakeup(&self) -> bool {
        unsafe { (*self.mmap.flags).load(Ordering::Relaxed) & XDP_RING_NEED_WAKEUP != 0 }
    }

    pub fn wake(&self) -> Result<u64, io::Error> {
        let result = unsafe { sendto(self.fd, ptr::null(), 0, libc::MSG_DONTWAIT, ptr::null(), 0) };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(result as u64)
    }

    pub fn capacity(&self) -> usize {
        self.size as usize
    }

    pub fn available(&self) -> usize {
        self.producer.available() as usize
    }

    pub fn commit(&mut self) {
        self.producer.commit();
    }

    pub fn sync(&mut self, commit: bool) {
        self.producer.sync(commit);
    }
}

pub struct RxRing {
    mmap: RingMmap<XdpDesc>,
    consumer: RingConsumer,
    size: u32,
    #[allow(dead_code)]
    fd: RawFd,
}

impl RxRing {
    fn new(mmap: RingMmap<XdpDesc>, size: u32, fd: RawFd) -> Self {
        debug_assert!(size.is_power_of_two());
        Self {
            consumer: RingConsumer::new(mmap.producer, mmap.consumer),
            mmap,
            size,
            fd,
        }
    }

    pub fn capacity(&self) -> usize {
        self.size as usize
    }

    pub fn available(&self) -> usize {
        self.consumer.available() as usize
    }

    pub fn read(&mut self) -> Option<ReceivedFrame> {
        let index = self.consumer.consume()? & self.size.saturating_sub(1);
        let desc = unsafe { self.mmap.desc.add(index as usize).read() };
        Some(ReceivedFrame {
            offset: RxFrameOffset(FrameOffset(desc.addr as usize)),
            len: desc.len as usize,
        })
    }

    pub fn commit(&mut self) {
        self.consumer.commit();
    }

    pub fn sync(&mut self, commit: bool) {
        self.consumer.sync(commit);
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{message}: {source}")]
    Syscall {
        message: String,
        #[source]
        source: io::Error,
    },
    #[error(
        "insufficient UMEM frames for RX fill ring prefill: required={required}, \
         available={available}"
    )]
    InsufficientUmemFrames { required: usize, available: usize },
}

impl Error {
    fn syscall(message: impl Into<String>, source: io::Error) -> Self {
        Self::Syscall {
            message: message.into(),
            source,
        }
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> io::Error {
        io::Error::other(error)
    }
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use {
        super::RxRing,
        crate::device::{RingMmap, XdpDesc},
        std::{
            mem, ptr,
            sync::atomic::{AtomicU32, Ordering},
        },
    };

    const DESC_OFFSET: usize = 64;

    // Anonymous mapping laid out like a kernel ring: producer @0, consumer @4, flags @8,
    // descriptors @DESC_OFFSET. RingMmap's Drop unmaps it.
    fn rx_ring(size: u32, start_index: u32) -> (RxRing, *mut AtomicU32, *mut XdpDesc) {
        let len = DESC_OFFSET + size as usize * mem::size_of::<XdpDesc>();
        // Safety: anonymous private mapping, checked against MAP_FAILED below.
        let base = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        assert!(!ptr::eq(base, libc::MAP_FAILED));
        // Safety: all offsets are within the `len` bytes mapped above.
        let (producer, consumer, flags, desc) = unsafe {
            (
                base.cast::<AtomicU32>(),
                base.add(4).cast::<AtomicU32>(),
                base.add(8).cast::<AtomicU32>(),
                base.add(DESC_OFFSET).cast::<XdpDesc>(),
            )
        };
        // Safety: producer and consumer point into the live mapping.
        unsafe {
            (*producer).store(start_index, Ordering::Release);
            (*consumer).store(start_index, Ordering::Release);
        }
        let mmap = RingMmap {
            mmap: base as *const u8,
            mmap_len: len,
            producer,
            consumer,
            desc,
            flags,
        };
        (RxRing::new(mmap, size, -1), producer, desc)
    }

    fn push(producer: *mut AtomicU32, desc: *mut XdpDesc, size: u32, addr: u64, len: u32) {
        // Safety: producer and desc point into the ring's live mapping; the index is masked.
        unsafe {
            let index = (*producer).load(Ordering::Relaxed);
            desc.add((index & (size - 1)) as usize).write(XdpDesc {
                addr,
                len,
                options: 0,
            });
            (*producer).store(index.wrapping_add(1), Ordering::Release);
        }
    }

    #[test]
    fn test_rx_ring_read() {
        let (mut ring, producer, desc) = rx_ring(4, 0);
        assert!(ring.read().is_none());

        push(producer, desc, 4, 4096 + 256, 64);
        push(producer, desc, 4, 8192 + 256, 1500);
        assert!(ring.read().is_none());
        ring.sync(false);

        let first = ring.read().unwrap();
        assert_eq!((first.offset.0.0, first.len), (4096 + 256, 64));
        let second = ring.read().unwrap();
        assert_eq!((second.offset.0.0, second.len), (8192 + 256, 1500));
        assert!(ring.read().is_none());
    }

    #[test]
    fn test_rx_ring_read_wrap_around() {
        let (mut ring, producer, desc) = rx_ring(4, u32::MAX - 1);
        for i in 0..3 {
            push(producer, desc, 4, i * 4096, i as u32 + 1);
        }
        ring.sync(false);

        for i in 0..3 {
            let frame = ring.read().unwrap();
            assert_eq!((frame.offset.0.0, frame.len), (i * 4096, i + 1));
        }
        assert!(ring.read().is_none());
    }
}
