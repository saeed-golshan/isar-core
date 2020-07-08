#[macro_export]
macro_rules! isar_try {
    { $($token:tt)* } => {{
        let mut l = || -> crate::error::Result<()> {
            $($token)*
            Ok(())
        };
        match l() {
            Ok(_) => 0,
            Err(e) => {
                eprintln!("{}",e);
                1
            },
        }
    }}
}
