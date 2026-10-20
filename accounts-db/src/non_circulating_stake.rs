//! Conservative stake candidates for non-circulating supply queries.
use {
    solana_account::ReadableAccount,
    solana_pubkey::Pubkey,
    solana_stake_interface::state::StakeStateV2,
    std::{collections::HashSet, sync::LazyLock},
};

static WITHDRAW_AUTHORITIES: LazyLock<HashSet<Pubkey>> =
    LazyLock::new(|| withdraw_authority().into_iter().collect());

/// Retain any stake that could be non-circulating at a nonnegative bank timestamp.
/// Expired lockups remain candidates: the querying bank must check its own clock.
/// Like owner indexes, candidates are a union across retained versions and forks.
pub(crate) fn is_candidate(account: &impl ReadableAccount) -> bool {
    if account.owner() != &solana_sdk_ids::stake::id() {
        return false;
    }
    match wincode::deserialize::<StakeStateV2>(account.data()).unwrap_or_default() {
        StakeStateV2::Initialized(meta) | StakeStateV2::Stake(meta, _, _) => {
            meta.lockup.epoch != 0
                || meta.lockup.unix_timestamp != 0
                || WITHDRAW_AUTHORITIES.contains(&meta.authorized.withdrawer)
        }
        _ => false,
    }
}

pub fn withdraw_authority() -> Vec<Pubkey> {
    [
        solana_pubkey::pubkey!("8CUUMKYNGxdgYio5CLHRHyzMEhhVRMcqefgE6dLqnVRK"),
        solana_pubkey::pubkey!("3FFaheyqtyAXZSYxDzsr5CVKvJuvZD1WE1VEsBtDbRqB"),
        solana_pubkey::pubkey!("FdGYQdiRky8NZzN9wZtczTBcWLYYRXrJ3LMDhqDPn5rM"),
        solana_pubkey::pubkey!("4e6KwQpyzGQPfgVr5Jn3g5jLjbXB4pKPa2jRLohEb1QA"),
        solana_pubkey::pubkey!("FjiEiVKyMGzSLpqoB27QypukUfyWHrwzPcGNtopzZVdh"),
        solana_pubkey::pubkey!("DwbVjia1mYeSGoJipzhaf4L5hfer2DJ1Ys681VzQm5YY"),
        solana_pubkey::pubkey!("GeMGyvsTEsANVvcT5cme65Xq5MVU8fVVzMQ13KAZFNS2"),
        solana_pubkey::pubkey!("Bj3aQ2oFnZYfNR1njzRjmWizzuhvfcYLckh76cqsbuBM"),
        solana_pubkey::pubkey!("4ZJhPQAgUseCsWhKvJLTmmRRUV74fdoTpQLNfKoekbPY"),
        solana_pubkey::pubkey!("HXdYQ5gixrY2H6Y9gqsD8kPM2JQKSaRiohDQtLbZkRWE"),
        solana_pubkey::pubkey!("4sNBQyPbJCQyUimBueZkGWnLVqds4rWkm7eXyi9WskGU"),
        solana_pubkey::pubkey!("AZWdNvnZxJnbcT8ZzonpN19AZJadxPdUxSiCEDTJzu8L"),
        solana_pubkey::pubkey!("5kFTzLuM2VgFdb6x16smnY3JWoVdPxNZVFAqeVgjSTUP"),
        solana_pubkey::pubkey!("Gbz6wkNFus8SNEkWGNNENLv9NFwVvF1pWVDpaVKUWcMh"),
        solana_pubkey::pubkey!("AksPzoA9DKCipgdhHjhUzQJe4iEniCvBoEfvayuFA3BN"),
        solana_pubkey::pubkey!("DVhs8YHWrvhhGxoefDNY9KotqtEEnjnSAK8MYGL2Q7X"),
        solana_pubkey::pubkey!("2bvGnYAPSV8pa1H3vRYr5tPAXktP4DkFACHfAgqyyfhd"),
        solana_pubkey::pubkey!("2xP8YQ3sVmfNPtGM17tZi7Lr4vsPUiN6mHLx42roazG5"),
        solana_pubkey::pubkey!("3p9ZxnrSFTkXVrT3KnYg2tT6asnysDApEFB5DRkdeAhB"),
        solana_pubkey::pubkey!("wRVP5MYuqP8HJ1Q8RCJ5NzUraL3DxCKPGMKSBd5iQH1"),
        solana_pubkey::pubkey!("513qFSVgmAQFBDsnyFCM1MrVKBrWiDgb4nXrdGsqa7Z4"),
        solana_pubkey::pubkey!("619qLS85ieR4qh7MGNLLZyLefN7hMi2FDVb5cmX1nisb"),
        solana_pubkey::pubkey!("768NcPfBJpFBtjDAbYZLFkej6ca1W5jeArsW5MtdF8S9"),
        solana_pubkey::pubkey!("EWAjC8a9VPbALSM3D6tGsbRfgDV48kRuHZPu8qtYSNDv"),
        solana_pubkey::pubkey!("H8wurbnaaXsgtrjqkNH1HhncUPUhTLAmKUHkwMeyqmfN"),
        solana_pubkey::pubkey!("HBfi37TwD4kMa1WrAWwXp3ZaFbZQ1g3XxWwNZs8QsCpY"),
        solana_pubkey::pubkey!("6B7mXMM6BixHvDpPAPLSKweLyCXcbtprkfsw3HfMUSjZ"),
        solana_pubkey::pubkey!("5TuV9WpmESXNfTNqasXVehoXpQy65WUHBbzgKPXPLwWx"),
        solana_pubkey::pubkey!("FB6VmiYFnVGp1uKXA3WbNsqg9neGYVpwYYiB9q8bFRrQ"),
        solana_pubkey::pubkey!("7Anoa4ZRiq8qaaiEnhmdpXyTEmBjZASXGfnQqVDynNie"),
        solana_pubkey::pubkey!("6dXeE5hS8bQKeqZsc18ewCyqHimnhCBAvVJBusnRqa2F"),
        solana_pubkey::pubkey!("Ds69ZQPb3D3aVPXdN5REyzALBrzJLdruJZ1cwyfyoEEx"),
        solana_pubkey::pubkey!("5HA8QV7tp59iNpfjs8f84LGUGX4imaynMFSKWHUcTrMT"),
        solana_pubkey::pubkey!("EPfiDzgbdgXdyqqwbYFMqU6Qvfx2J9Zf2J9noXBYYbbx"),
        solana_pubkey::pubkey!("DEGyTHFXmYuyANRDYRoEcShBXLonWNdBRpNzbFZBuzhY"),
        solana_pubkey::pubkey!("9xz1vZSWgY6TFPLZgPM2WJjDM1KiPXcALnjYFHRTkYiK"),
        solana_pubkey::pubkey!("6n1mSmsdGFCkEyyHe4wtgEigbwhiwYRszrerMW9YRyYF"),
        solana_pubkey::pubkey!("8rp9vcJG1Uwk25g9XfQNHysfGBYcHizVEJvavqiffB6P"),
        solana_pubkey::pubkey!("BVsJuJbhXmxPB9XPZrYMXs1SfuZuo998u1jQ9emVp8YD"),
        solana_pubkey::pubkey!("Fas9FrnngyVjdMZ6r8p8SsfyFDtHLMmrw1dyBxRWWVTF"),
        solana_pubkey::pubkey!("6Lfk4e6yxXGcFNMHue5u8KQM9hWYKjbdMFZ2n7SHHd6L"),
        solana_pubkey::pubkey!("6WuU6DWoM7ZpgUVL5TR97Fgose7vLgrv4Qvg1Yhqv8Xw"),
        solana_pubkey::pubkey!("6mXiGJbQbX726ZhrMSmbLCF81E8rkSfo1pvBWyn1asG5"),
        solana_pubkey::pubkey!("2UdDthRdtZKMjEx9oQYnzuLdWy3sa4XKeaqihdXkXFza"),
        solana_pubkey::pubkey!("33XwSCq2Ft5tTCsGK7kTyMCJ4hqufDZT2REFAWXmsia8"),
        solana_pubkey::pubkey!("B5tRZWRCTCLANkY5zLnxEXAn8S2exJD9EkH3cacEYqmE"),
        solana_pubkey::pubkey!("CfCvwXZk1sLSqzuoFx1K7KG1puMofgwHA3tSpcArVysX"),
        solana_pubkey::pubkey!("C4fATCN93YL5iYS3c2MrHeDmkMD6waHuBXLqsri5q5AF"),
        solana_pubkey::pubkey!("HbJUXNHXLzpucDr9wgjuoqAwK6XNKEcqi5h29MkkuSBQ"),
        solana_pubkey::pubkey!("G8EBx1Qo5W8731nmaBSGYyvU96onpEP5adrVSjt4vnLF"),
        solana_pubkey::pubkey!("4dc5ty7TNod2EQfx4bPnDofPbQk5cELcn3it47qnLizL"),
        solana_pubkey::pubkey!("4VCET97mE8ixxAPRYabM7S7dvkiAFZ8ZzEoH6kbmfYus"),
        solana_pubkey::pubkey!("DwqBsPgtAHqQUmRN3WF5YTuLKhK7dqzrb9eRJ4UVRNLf"),
        solana_pubkey::pubkey!("26FqcAbTLvr6cVXk2ktPu9hpeMYWukY7uUwNCtTwu7Wb"),
        solana_pubkey::pubkey!("B22zqCsZL6PDsi4fftmcjj5k7hFjhH1BM8Dcz4fXfFXK"),
        solana_pubkey::pubkey!("9ahLw5LFpeTuBykK3A5aiFZ1CidTY7z4YxG2kmQdSGon"),
        solana_pubkey::pubkey!("8M731ZthMdeVoMFGRaPnUBzR7Ze6qY4zEcQ7WALEAn5i"),
        solana_pubkey::pubkey!("A3aaVsuX7BfNCUQbuepQSaLs9KNB7c6djG6u4UuJzwwP"),
        solana_pubkey::pubkey!("DeXE7LTUqsC3B9kSACfFdW5TPG1eT9n1Z6gVB9dJNxqz"),
        solana_pubkey::pubkey!("BtvSeDfZNfA3VEH6baFprfx6JcS6JX8jdt6NffCxoJGV"),
        solana_pubkey::pubkey!("8o8trB6XdG8o4qo29De2mcooY5E3pW2rvMMkeBwJHkUD"),
        solana_pubkey::pubkey!("CSSxN8vQYqSjg5GV6ZUGSM65dXqhytLzANLpMrBZjLKP"),
        solana_pubkey::pubkey!("5A2yoxdGdC3B3QC39pkXpjx2ZPH4WGgie9exHQnhNz4J"),
        solana_pubkey::pubkey!("DFaHYDcuLmxx3vrJGCgw8sPehzBNjvbvZTjz7PgvYLcb"),
        solana_pubkey::pubkey!("43Cwv866ActL11JqVQXoFPiytUYA3tFXUaNg1zPsarn9"),
        solana_pubkey::pubkey!("3wzw3L4wd3B7DHocbQxHosQXEfqEz5AnkQHKBisGKLzY"),
        solana_pubkey::pubkey!("EnX9MvycxMGKFpMPJHgNo6kD2proVaorKQcSaMmguMEZ"),
        solana_pubkey::pubkey!("GLz4zxemfHJy3BnK7PjZ4uUgJoHtycpT3JrmTDYduVpg"),
        solana_pubkey::pubkey!("362rDzQwChHJ9ToZgLdv8wzFHTTsxsxzFLTZPwKY3CYg"),
        solana_pubkey::pubkey!("3FKwgoQWCz4i43Pbo3jdfsjzkgrkjnk44rDWh7A9J9Ee"),
        solana_pubkey::pubkey!("Cj53e3n1WX9dAUNX5PZpsc9xG9e7hXGnuJkKLJkNaf6v"),
        solana_pubkey::pubkey!("Amn9KQZY5kJj932b49pxwqZuuKfNWewrmNQtdMrRfdvf"),
        solana_pubkey::pubkey!("6spvvQJzFCcw5fVVdF1zKxCeq8Gj2yRFGgFuG16fYSMo"),
        solana_pubkey::pubkey!("H5PutDz8EgAoznaX7Bc8qSB6sPxD2Xho145H6MH6AcbV"),
        solana_pubkey::pubkey!("HNCooAiRiVeqGK9VSjxsvRsfr4weTcx14XhkvM7zDMgx"),
        solana_pubkey::pubkey!("AUqLkd6bR54NtXY9Zt4J2zZ7CJbhVHuDR95YFk5qahaX"),
        solana_pubkey::pubkey!("H35YRuMGE6WDPBLu3fDYLxV7tEcL2yJ7C97jDkw6NrBf"),
        solana_pubkey::pubkey!("EhN8bv8LCXnNAVju9ERGsCccmdBNmNTAESAHXE8PyBLR"),
        solana_pubkey::pubkey!("Ht5roME7Fjzt3dcEyf7UxYpPPAAdyTrnJ6rgEwQrNAFg"),
        solana_pubkey::pubkey!("5zHRch6mowPiobRQHYXZ1siRcthw4dYkFPuaNDQkK3EE"),
        solana_pubkey::pubkey!("AZkdkkHzSEbaZXpr7xBmt71AHj9c8iWE2oWYELm3nns5"),
        solana_pubkey::pubkey!("FThHNjNQYi7FLvP4KjeYf3LXjcN2LhyYHmza35ucqtDT"),
        solana_pubkey::pubkey!("6vHscAGjgbTfi1ooRVWGjSiUhHcUnLBceJyuKjpKrCYy"),
        solana_pubkey::pubkey!("AnbszMCtjEVZTquG3FQ68x1f1WK5B1Z6E7gtN2J6vD5a"),
        solana_pubkey::pubkey!("5aUknZidNKmAPxLMHoUeWsEdmXX8sGKvj8s71pHTDC7H"),
        solana_pubkey::pubkey!("7Xgby7EgwoRkhbQSdNJL9sXwpo3jddUhqB4zv3WuJB4m"),
        solana_pubkey::pubkey!("43KdfhYK6LUXX4LmyxfZ1KLaQrfpRe8vNwopNS7wXsUC"),
        solana_pubkey::pubkey!("FG6UGgT4VpCQxjwQsVy6woHeemwb5jP5u9kR5ry3twcK"),
        solana_pubkey::pubkey!("BDvLyqobZFubd7ESEJde6KqjbNoChkdj5aEiBdCqcN5r"),
        solana_pubkey::pubkey!("BtF4msVoQYJPrQUMwNtEXw58LVjux18ddVhC4XjbsdwX"),
        solana_pubkey::pubkey!("GbPK6cgUKso2ZGNV6GRwbp3hkdUzTVjsybYQMGDBvMFa"),
        solana_pubkey::pubkey!("4bWjzNek2m6P3Q8rKkq11C47TaoAwpj3ykk5yDCYLWr1"),
        solana_pubkey::pubkey!("FPzPT7YV5BHSTjYjqycXbwBsD4E5WsrZNPcCgcHhPkUx"),
        solana_pubkey::pubkey!("EbitXxcwjC1dzrGJ7sa3Y6RBzSM3wxvrxMaBncNBKEf2"),
        solana_pubkey::pubkey!("HMmHHmCmC3oo4G4QCjKvNxctSBQzRFDp2WQHZoXyrW6Q"),
        solana_pubkey::pubkey!("145FLxWrDG9gKifyJKSnkKNAUPhG6hxUMEiZWw6XeSMW"),
        solana_pubkey::pubkey!("FnXgwJZT7RMhpYEJFCXGLHp63bhVLfVn5d4aSVQDe19n"),
        solana_pubkey::pubkey!("5tDptzMGG79DLp9XV4fdcmiRNPQGCkfBJeo8EVCSYKw7"),
        solana_pubkey::pubkey!("9oBNYSARUMuH2swnxzbxCi827YfUnYWyWSAkrHz93h5y"),
        solana_pubkey::pubkey!("DWfXRxmUXKdSTji4S1nXncviTmrhWQwxHAQCcvjRLeWm"),
        solana_pubkey::pubkey!("EHUv6Hc9XGAb57C8iNCUuoE9xnJnRTFr9LaP7zbu8LNB"),
        solana_pubkey::pubkey!("CTqnGTKHrcg8AVK5pSwVzpA3mmSxz7Js9y7rEqCbjryH"),
        solana_pubkey::pubkey!("7PzsZ3ApLcfQosSr2B1C9YV1GNNgriZVFyqzDpeHoAD"),
        solana_pubkey::pubkey!("AKXpgmVhedTT49sekGbh6MXfx2kJKBcaFQLpc83NCp6N"),
        solana_pubkey::pubkey!("3MP45RTcFdLX3HpPD18asXCCxFjsgrZM9V6tEeQxFXd4"),
        solana_pubkey::pubkey!("88YC29WwpugANLcN7DYkuL3benW79WsdxkQ3KX8zER9s"),
        solana_pubkey::pubkey!("98j2yMfxrLzQ5YQJU1FMW3CTkoYgZStJirpm4HLAkA3o"),
        solana_pubkey::pubkey!("56AidDLicyQaLMqbMQeZ7itUmJyd7TAvehVDi8MXDYbx"),
        solana_pubkey::pubkey!("3yeCpRbBU5gzBaweVRqUXFcZUp7gSXBeknKcbGmjwmjJ"),
        solana_pubkey::pubkey!("5McU7LcsRw3RHyrjj98qHefx6ehy7MdjF1uKKi6Kp8hV"),
        solana_pubkey::pubkey!("3CYxk8uTXtANvbU1HW3NHNhKcBEfYesi2BdBFizy3f6s"),
        solana_pubkey::pubkey!("4bAowZuPRG8jzNwMceqjYjZnrGXQNR5ZCgQJM8s4hF4i"),
        solana_pubkey::pubkey!("ATWFW8xBGPe6xB8JnjyjZqG5FuEXnfyuhEocmqAqfeJG"),
        solana_pubkey::pubkey!("73Aa7yCd6AMPqsaLisfgrpjESVFtDrLnHEjfDsXBK3HV"),
    ]
    .into()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_account::Account,
        solana_stake_interface::{
            stake_flags::StakeFlags,
            state::{Authorized, Lockup, Meta, Stake},
        },
    };

    fn account(state: StakeStateV2) -> Account {
        let mut data = wincode::serialize(&state).unwrap();
        data.resize(StakeStateV2::size_of(), 0);
        Account {
            lamports: 1,
            owner: solana_sdk_ids::stake::id(),
            data,
            ..Account::default()
        }
    }

    #[test]
    fn test_candidate_lockups_authorities_and_padded_states() {
        let unlocked = Meta::default();
        assert!(!is_candidate(&account(StakeStateV2::Initialized(unlocked))));
        assert!(!is_candidate(&account(StakeStateV2::Stake(
            unlocked,
            Stake::default(),
            StakeFlags::empty()
        ))));
        for lockup in [
            Lockup {
                epoch: 1,
                ..Lockup::default()
            },
            Lockup {
                unix_timestamp: 1,
                ..Lockup::default()
            },
        ] {
            let meta = Meta {
                lockup,
                ..Meta::default()
            };
            assert!(is_candidate(&account(StakeStateV2::Initialized(meta))));
            assert!(is_candidate(&account(StakeStateV2::Stake(
                meta,
                Stake::default(),
                StakeFlags::empty()
            ))));
        }
        let meta = Meta {
            authorized: Authorized::auto(&withdraw_authority()[0]),
            ..Meta::default()
        };
        assert!(is_candidate(&account(StakeStateV2::Initialized(meta))));
        assert!(is_candidate(&account(StakeStateV2::Stake(
            meta,
            Stake::default(),
            StakeFlags::empty()
        ))));
    }

    #[test]
    fn test_candidate_ignores_invalid_states_and_owners() {
        assert!(!is_candidate(&account(StakeStateV2::Uninitialized)));
        let mut valid = account(StakeStateV2::Initialized(Meta {
            lockup: Lockup {
                epoch: 1,
                ..Lockup::default()
            },
            ..Meta::default()
        }));
        valid.owner = Pubkey::new_unique();
        assert!(!is_candidate(&valid));
        valid.owner = solana_sdk_ids::stake::id();
        valid.data = vec![255];
        assert!(!is_candidate(&valid));
    }
}
