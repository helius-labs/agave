use {
    crate::handshake::{
        AgaveHandshakeError, ClientHandshakeError, ClientLogon,
        client::connect,
        server::Server,
        shared::{LOGON_FAILURE, MAX_WORKERS, VERSION},
    },
    agave_scheduler_bindings::{
        PackToSimulationWorkerMessage, PackToWorkerMessage, ProgressMessage,
        SharableTransactionBatchRegion, SharableTransactionRegion, SimulationResponseRegion,
        SimulationWorkerToPackMessage, TpuToPackMessage, TransactionResponseRegion,
        WorkerToPackMessage,
    },
    std::{
        io::{Read, Write},
        os::unix::net::UnixStream,
        time::Duration,
    },
    tempfile::NamedTempFile,
};

#[test]
fn protocol_v5_is_rejected_and_v6_is_accepted() {
    let ipc = NamedTempFile::new().unwrap();
    let path = ipc.path().to_path_buf();
    std::fs::remove_file(&path).unwrap();
    let mut server = Server::new(&path).unwrap();
    let logon = ClientLogon {
        worker_count: 1,
        allocator_size: 64 * 1024 * 1024,
        allocator_handles: 1,
        tpu_to_pack_capacity: 16,
        progress_tracker_capacity: 16,
        pack_to_worker_capacity: 16,
        worker_to_pack_capacity: 16,
        simulation_worker_count: 1,
        pack_to_simulation_worker_capacity: 16,
        simulation_worker_to_pack_capacity: 16,
        flags: 0,
    };

    let server_handle = std::thread::spawn(move || {
        assert!(matches!(
            server.accept(),
            Err(AgaveHandshakeError::Version {
                server: VERSION,
                client: 5
            })
        ));
        assert!(server.accept().is_ok());
    });

    let mut stream = UnixStream::connect(&path).unwrap();
    let mut buffer = [0_u8; 1024];
    buffer[..8].copy_from_slice(&5_u64.to_le_bytes());
    const LOGON_END: usize = 8 + core::mem::size_of::<ClientLogon>();
    // SAFETY: the fixed buffer is large enough and the destination may be unaligned.
    unsafe { core::ptr::write_unaligned(buffer[8..LOGON_END].as_mut_ptr().cast(), logon) };
    stream.write_all(&buffer).unwrap();
    let mut response = [0_u8; 256];
    let response_len = stream.read(&mut response).unwrap();
    assert!(response_len > 0);
    assert_eq!(response[0], LOGON_FAILURE);
    drop(stream);

    assert!(connect(path, logon, Duration::from_secs(1)).is_ok());
    server_handle.join().unwrap();
}

#[test]
fn message_passing_on_all_queues() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    // Test messages.
    let tpu_to_pack = TpuToPackMessage {
        transaction: SharableTransactionRegion {
            offset: 10,
            length: 5,
        },
        flags: 21,
        src_addr: [4; 16],
    };
    let progress_tracker = ProgressMessage {
        leader_state: agave_scheduler_bindings::LEADER_READY,
        current_slot_progress: 32,
        epoch: 7,
        current_slot: 3,
        next_leader_slot: 12,
        leader_range_end: 16,
        remaining_cost_units: 12_000_000,
        latest_blockhash: [42; 32],
    };
    let pack_to_worker = PackToWorkerMessage {
        flags: 123,
        max_working_slot: 100,
        batch: SharableTransactionBatchRegion {
            num_transactions: 5,
            transactions_offset: 100,
        },
    };
    let worker_to_pack = WorkerToPackMessage {
        batch: SharableTransactionBatchRegion {
            num_transactions: 5,
            transactions_offset: 100,
        },
        processed_code: agave_scheduler_bindings::processed_codes::PROCESSED,
        responses: TransactionResponseRegion {
            tag: 3,
            num_transaction_responses: 2,
            transaction_responses_offset: 1,
        },
    };
    let pack_to_simulation_worker = PackToSimulationWorkerMessage {
        flags: 0,
        max_working_slot: 101,
        batch: pack_to_worker.batch,
    };
    let simulation_worker_to_pack = SimulationWorkerToPackMessage {
        batch: pack_to_worker.batch,
        processed_code: agave_scheduler_bindings::processed_codes::PROCESSED,
        responses: SimulationResponseRegion {
            num_transaction_responses: 5,
            transaction_responses_offset: 200,
        },
    };

    let server_handle = std::thread::spawn(move || {
        let mut session = server.accept().unwrap();

        // Send a tpu_to_pack message.
        session.tpu_to_pack.producer.try_write(tpu_to_pack).unwrap();
        session.tpu_to_pack.producer.commit();

        // Send a progress_tracker message.
        session
            .progress_tracker
            .try_write(progress_tracker)
            .unwrap();
        session.progress_tracker.commit();

        let msg = loop {
            if let Some(msg) = session.simulation_workers[0]
                .pack_to_simulation_worker
                .try_read()
            {
                break msg;
            }
        };
        assert_eq!(msg, pack_to_simulation_worker);
        session.simulation_workers[1]
            .simulation_worker_to_pack
            .try_write(simulation_worker_to_pack)
            .unwrap();

        // Receive pack_to_worker messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            let msg = loop {
                worker.pack_to_worker.sync();
                if let Some(msg) = worker.pack_to_worker.try_read() {
                    break *msg;
                }
            };
            assert_eq!(
                PackToWorkerMessage {
                    max_working_slot: pack_to_worker.max_working_slot + i as u64,
                    ..pack_to_worker
                },
                msg
            );
        }

        // Send worker_to_pack messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            worker
                .worker_to_pack
                .try_write(WorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: worker_to_pack.batch.num_transactions + i as u8,
                        ..worker_to_pack.batch
                    },
                    ..worker_to_pack
                })
                .unwrap();
            worker.worker_to_pack.commit();
        }
    });
    let client_handle = std::thread::spawn(move || {
        let mut session = connect(
            ipc,
            ClientLogon {
                worker_count: 4,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                simulation_worker_count: 2,
                pack_to_simulation_worker_capacity: 1024,
                simulation_worker_to_pack_capacity: 1024,
                flags: 0,
            },
            Duration::from_secs(1),
        )
        .unwrap();

        // Receive tpu_to_pack message.
        let msg = loop {
            session.tpu_to_pack.sync();
            if let Some(msg) = session.tpu_to_pack.try_read() {
                break *msg;
            };
        };
        assert_eq!(msg, tpu_to_pack);

        // Receive progress_tracker message.
        let msg = loop {
            session.progress_tracker.sync();
            if let Some(msg) = session.progress_tracker.try_read() {
                break *msg;
            };
        };
        assert_eq!(msg, progress_tracker);

        session
            .pack_to_simulation_worker
            .try_write(pack_to_simulation_worker)
            .unwrap();
        let msg = loop {
            if let Some(msg) = session.simulation_worker_to_pack.try_read() {
                break msg;
            }
        };
        assert_eq!(msg, simulation_worker_to_pack);

        // Send pack_to_worker messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            worker
                .pack_to_worker
                .try_write(PackToWorkerMessage {
                    max_working_slot: pack_to_worker.max_working_slot + i as u64,
                    ..pack_to_worker
                })
                .unwrap();
            worker.pack_to_worker.commit();
        }

        // Receive worker_to_pack messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            let msg = loop {
                worker.worker_to_pack.sync();
                if let Some(msg) = worker.worker_to_pack.try_read() {
                    break *msg;
                }
            };
            assert_eq!(
                WorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: worker_to_pack.batch.num_transactions + i as u8,
                        ..worker_to_pack.batch
                    },
                    ..worker_to_pack
                },
                msg
            );
        }
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn accept_worker_count_max() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        assert!(res.is_ok());
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: MAX_WORKERS,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                simulation_worker_count: 0,
                pack_to_simulation_worker_capacity: 1,
                simulation_worker_to_pack_capacity: 1,
                flags: 0,
            },
            Duration::from_secs(1),
        );
        assert!(res.is_ok());
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_worker_count_low() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::WorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 0);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 0,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                simulation_worker_count: 0,
                pack_to_simulation_worker_capacity: 1,
                simulation_worker_to_pack_capacity: 1,
                flags: 0,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Worker count; count=0");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_worker_count_high() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::WorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 100);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 100,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                simulation_worker_count: 0,
                pack_to_simulation_worker_capacity: 1,
                simulation_worker_to_pack_capacity: 1,
                flags: 0,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Worker count; count=100");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}
