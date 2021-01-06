pub type DartPort = i64;

extern "C" {
    #[link_name = "Dart_PostInteger_DL"]
    pub fn dart_post_int(port: DartPort, value: i64) -> bool;
}
